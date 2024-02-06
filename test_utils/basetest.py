import unittest
import os
import json
import datetime
import subprocess
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource.resources.models import Deployment, DeploymentMode
from sumologic import SumoLogic
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.loganalytics import LogAnalyticsManagementClient

class BaseTest(unittest.TestCase):
    
    def setUp(self):
        # azure credentials
        self.azure_credential = DefaultAzureCredential()
        self.subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
        self.resourcegroup_location = os.environ["AZURE_DEFAULT_REGION"]

        self.resource_client = ResourceManagementClient(
            self.azure_credential, self.subscription_id)
        self.repo_name, self.branch_name = self.get_git_info()

        # sumologic: collector and source
        self.sumologic_cli = SumoLogic(
            os.environ["SUMO_ACCESS_ID"], os.environ["SUMO_ACCESS_KEY"], self.api_endpoint(os.environ["SUMO_DEPLOYMENT"]))
        self.collector_id = self.create_collector(self.collector_name)
        self.sumo_source_id, self.sumo_endpoint_url = self.create_source(
            self.collector_id, self.source_name)

    def resource_group_exists(self, group_name):
        # grp: name,id,properties

        for grp in self.resource_client.resource_groups.list():
            if grp.name == group_name:
                if grp.properties.provisioning_state == "Succeeded":
                    return True
                else:
                    print("Error", getattr(grp.properties, 'error', None))

        return False

    def create_resource_group(self):
        resource_group_params = {'location': self.resourcegroup_location}
        resp = self.resource_client.resource_groups.create_or_update(
            self.RESOURCE_GROUP_NAME, resource_group_params)
        print('Creating ResourceGroup: {}'.format(
            self.RESOURCE_GROUP_NAME), resp.properties.provisioning_state)
        
    def get_resource(self, restype):
        for item in self.resource_client.resources.list_by_resource_group(self.RESOURCE_GROUP_NAME):
            if (item.type == restype):
                return item
        raise Exception("%s Resource Not Found" % (restype))

    def get_resource_name(self, resprefix, restype):
        for item in self.resource_client.resources.list_by_resource_group(self.RESOURCE_GROUP_NAME):
            if (item.name.startswith(resprefix) and item.type == restype):
                return item.name
        raise Exception("%s Resource Not Found" % (resprefix))
    
    def get_resources(self, resource_group_name):
        return self.resource_client.resources.list_by_resource_group(resource_group_name)

    def get_Workspace_Id(self):
        workspace = self.get_resource(
            'microsoft.operationalinsights/workspaces')
        client = LogAnalyticsManagementClient(
            credential=self.azure_credential,
            subscription_id=self.subscription_id,
        )

        response = client.workspaces.get(
            resource_group_name=self.RESOURCE_GROUP_NAME,
            workspace_name=workspace.name,
        )
        return response.customer_id


    def delete_resource_group(self):
            resp = self.resource_client.resource_groups.begin_delete(self.RESOURCE_GROUP_NAME)
            resp.wait()
            print('Deleted ResourceGroup:{}'.format(self.RESOURCE_GROUP_NAME), resp.status())

    def deploy_template(self):
            print("Deploying template")
            deployment_name = "%s-Test-%s" % (datetime.datetime.now().strftime("%d-%m-%y-%H-%M-%S"), self.RESOURCE_GROUP_NAME)
            template_data = self._parse_template()

            deployment_properties = {
                'mode': DeploymentMode.INCREMENTAL,
                'template': template_data
            }

            deployment = Deployment(properties=deployment_properties)

            deployment_operation_poller = self.resource_client.deployments.begin_create_or_update(
                self.RESOURCE_GROUP_NAME,
                deployment_name,
                deployment
            )
            
            deployment_result = deployment_operation_poller.result()
            print(f"ARM Template deployment completed with result: {deployment_result}")

    def get_git_info(self):
        repo_slug = "SumoLogic/sumologic-azure-function"
        try:
            branch_name = subprocess.check_output("git branch --show-current", stderr=subprocess.STDOUT, shell=True)
            branch_name = self.branch_name.decode("utf-8").strip()
        
        except Exception:
            branch_name = os.environ["SOURCE_BRANCH"]
            
        if not branch_name or branch_name == "undefined":
            raise Exception("Error getting branch name")

        repo_name = f"https://github.com/{repo_slug}"
        
        print(f"Testing for repo {repo_name} in branch {branch_name}")

        return repo_name, branch_name

    def api_endpoint(self, sumo_deployment):
        if sumo_deployment == "us1":
            return "https://api.sumologic.com/api"
        elif sumo_deployment in ["ca", "au", "de", "eu", "jp", "us2", "fed", "in"]:
            return "https://api.%s.sumologic.com/api" % sumo_deployment
        else:
            return 'https://%s-api.sumologic.net/api' % sumo_deployment
        
    def create_collector(self, collector_name):
        print("create_collector start")
        collector_id = None
        collector = {
                    'collector': {
                        'collectorType': 'Hosted',
                        'name': collector_name,
                        'description': "",
                        'category': None
                    }
                }
        try:
            resp = self.sumologic_cli.create_collector(collector, headers=None)
            collector_id = json.loads(resp.text)['collector']['id']
            print("created collector")
        except Exception as e:
            raise Exception(e)

        return collector_id
    
    def delete_collector(self, collector_id):
        sources = self.sumologic_cli.sources(collector_id, limit=10)
        if len(sources) == 0:
            self.sumologic_cli.delete_collector({"collector": {"id": collector_id}})
            print("deleted collector")
    
    def create_source(self, collector_id, source_name):
        print("create_source start")
        endpoint = source_id = None
        params = {
            "sourceType": "HTTP",
            "name": source_name,
            "messagePerRequest": False,
            "multilineProcessingEnabled": True,
            "category": "AZURE/UnitTest/logs"
        }

        try:
            resp = self.sumologic_cli.create_source(collector_id, {"source": params})
            data = resp.json()['source']
            source_id = data["id"]
            endpoint = data["url"]
            print("created source")
        except Exception as e:
            raise Exception(e)
        return source_id, endpoint
    
    def delete_source(self, collector_id, source_id):
        self.sumologic_cli.delete_source(collector_id, {"source": {"id": source_id}})
        print("deleted source")