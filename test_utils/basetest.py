import unittest
import os
import json
import datetime
import subprocess
from azure.mgmt.resource.resources.models import Deployment

class BaseTest(unittest.TestCase):

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
        try:
            resp = self.resource_client.resource_groups.begin_delete(self.RESOURCE_GROUP_NAME)
            resp.wait()
            print('Deleting {}'.format(self.RESOURCE_GROUP_NAME), resp.status())
        except Exception as e:
            print("An unexpected error occurred during the delete_resource_group:")
            print("Exception", e)

    def create_resource_group(self):
        try:
            resource_group_params = {'location': self.namespace_location}
            resp = self.resource_client.resource_groups.create_or_update(self.RESOURCE_GROUP_NAME, resource_group_params)
            print('Creating {}'.format(self.RESOURCE_GROUP_NAME), resp.properties.provisioning_state)
        except Exception as e:
            print("An unexpected error occurred during the create_resource_group:")
            print("Exception", e)

    def deploy_template(self):
        try:
            deployment_name = "%s-Test-%s" % (datetime.datetime.now().strftime("%d-%m-%y-%H-%M-%S"), self.RESOURCE_GROUP_NAME)
            
            template_file_path = os.path.join(os.path.abspath('..'), 'src', self.template_name)
            parameters = {
                'SumoEndpointURL': {'defaultValue': self.sumo_endpoint_url},
                'sourceCodeBranch': {'defaultValue': self.branch_name},
                'sourceCodeRepositoryURL': {'defaultValue': self.repo_name}
            }

            # Read the template file
            with open(template_file_path, 'r') as template_file:
                template_content = template_file.read()

            # Define the deployment properties
            deployment_properties = {
                'mode': 'Incremental',
                'template': template_content,
                'parameters': parameters
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

        except Exception as e:
            print("An unexpected error occurred during the deploy_template:")
            print("Exception", e)

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
