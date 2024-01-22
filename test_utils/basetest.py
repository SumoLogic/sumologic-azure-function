import unittest
import os
import json
import datetime
import subprocess
from sumologic import SumoLogic
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource.resources.models import Deployment, DeploymentMode


class BaseTest(unittest.TestCase):
    
    def create_credentials(self):
        self.azure_credential = DefaultAzureCredential()
        self.subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
        self.resourcegroup_location = os.environ["AZURE_DEFAULT_REGION"]

    def resource_group_exists(self, group_name):
        # grp: name,id,properties

        for grp in self.resource_client.resource_groups.list():
            if grp.name == group_name:
                if grp.properties.provisioning_state == "Succeeded":
                    return True
                else:
                    print("Error", getattr(grp.properties, 'error', None))

        return False

    def delete_resource_group(self):
            resp = self.resource_client.resource_groups.begin_delete(self.RESOURCE_GROUP_NAME)
            resp.wait()
            print('Deleted ResourceGroup:{}'.format(self.RESOURCE_GROUP_NAME), resp.status())

    def create_resource_group(self):
            resource_group_params = {'location': self.resourcegroup_location}
            resp = self.resource_client.resource_groups.create_or_update(self.RESOURCE_GROUP_NAME, resource_group_params)
            print('Creating ResourceGroup: {}'.format(self.RESOURCE_GROUP_NAME), resp.properties.provisioning_state)

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
        if os.environ.get("TRAVIS_EVENT_TYPE") == "pull_request":
            branch_name = os.environ["TRAVIS_PULL_REQUEST_BRANCH"]
            repo_slug = os.environ["TRAVIS_PULL_REQUEST_SLUG"]
        elif os.environ.get("TRAVIS_EVENT_TYPE") == "push":
            branch_name = os.environ["TRAVIS_BRANCH"]
            repo_slug = os.environ["TRAVIS_REPO_SLUG"]
        else:
            git_cmd = "git rev-parse --abbrev-ref HEAD" # will not work in detached state
            branch_name = subprocess.Popen(git_cmd, shell=True, stdout=subprocess.PIPE).stdout.read().strip()

        repo_name = "https://github.com/%s" % (repo_slug)
        if not branch_name or branch_name == "undefined" or not repo_name:
            raise Exception("No branch Found")
        print("Testing for repo %s in branch %s" % (repo_name, branch_name))

        if isinstance(branch_name, bytes):
            branch_name = branch_name.decode()

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
        self.sumologic_cli = SumoLogic(os.environ["SUMO_ACCESS_ID"], os.environ["SUMO_ACCESS_KEY"], self.api_endpoint(os.environ["SUMO_DEPLOYMENT"]))
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
            print(f"created collector {collector_id}")
        except Exception as e:
            raise Exception(e)

        return collector_id
    
    def delete_collector(self, collector_id):
        sources = self.sumologic_cli.sources(collector_id, limit=10)
        if len(sources) == 0:
            self.sumologic_cli.delete_collector({"collector": {"id": collector_id}})
            print(f"deleted collector {collector_id}")
    
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
            print(f"created source '{source_id}' endpoint '{endpoint}'")
        except Exception as e:
            raise Exception(e)
        return source_id, endpoint
    
    def delete_source(self, collector_id, source_id):
        self.sumologic_cli.delete_source(collector_id, {"source": {"id": source_id}})
        print(f"deleted source {source_id}")