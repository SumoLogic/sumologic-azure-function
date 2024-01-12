import unittest
import os
import json
import datetime
import subprocess
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource.resources.models import Deployment,DeploymentMode

class BaseTest(unittest.TestCase):
    
    def create_credentials(self):
        self.azure_credential = DefaultAzureCredential()
        self.subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
        self.resourcegroup_location = os.getenv("AZURE_DEFAULT_REGION")

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
            deployment_name = "%s-Test-%s" % (datetime.datetime.now().strftime("%d-%m-%y-%H-%M-%S"), self.RESOURCE_GROUP_NAME)
            template_data = self._parse_template()

            # Define the deployment properties
            deployment_properties = {
                'mode': DeploymentMode.INCREMENTAL,
                'template': template_data
            }

            # Create a Deployment object
            deployment = Deployment(properties=deployment_properties)

            # Begin deployment
            deployment_operation_poller = self.resource_client.deployments.begin_create_or_update(
                self.RESOURCE_GROUP_NAME,
                deployment_name,
                deployment
            )
            
            # Wait for the deployment to complete
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
