import unittest
import os
import json
import datetime
import subprocess
from azure.mgmt.resource.resources.models import DeploymentMode
from azure.common.credentials import ServicePrincipalCredentials


class BaseTest(unittest.TestCase):

    def create_credentials(self):
        config_file = os.path.expanduser("~/.azure/azure_credentials.json")
        if os.path.isfile(config_file):
            self.config = json.load(open(config_file))
        else:
            self.config = {
                'AZURE_SUBSCRIPTION_ID': os.environ['AZURE_SUBSCRIPTION_ID'],
                'AZURE_CLIENT_ID': os.environ['AZURE_CLIENT_ID'],
                'AZURE_CLIENT_SECRET': os.environ['AZURE_CLIENT_SECRET'],
                'AZURE_TENANT_ID': os.environ['AZURE_TENANT_ID'],
                'AZURE_DEFAULT_REGION': os.environ.get("AZURE_DEFAULT_REGION",
                                                       "westus")
            }
        self.subscription_id = str(self.config['AZURE_SUBSCRIPTION_ID'])

        self.credentials = ServicePrincipalCredentials(
            client_id=self.config['AZURE_CLIENT_ID'],
            secret=self.config['AZURE_CLIENT_SECRET'],
            tenant=self.config['AZURE_TENANT_ID']
        )
        self.location = str(self.config['AZURE_DEFAULT_REGION'])

        print("creating credentials", self.subscription_id)

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
        resource_group_params = {'location': self.location}
        resp = self.resource_client.resource_groups.delete(
            self.RESOURCE_GROUP_NAME, resource_group_params)
        resp.wait()
        print('Deleting {}'.format(self.RESOURCE_GROUP_NAME), resp.status())

    def create_resource_group(self):
        resource_group_params = {'location': self.location}

        resp = self.resource_client.resource_groups.create_or_update(
            self.RESOURCE_GROUP_NAME, resource_group_params)
        print('Creating {}'.format(
            self.RESOURCE_GROUP_NAME),
            resp.properties.provisioning_state)

    def deploy_template(self):
        print("Deploying template")
        deployment_name = "%s-Test-%s" % (datetime.datetime.now().strftime(
            "%d-%m-%y-%H-%M-%S"), self.RESOURCE_GROUP_NAME)
        template_data = self._parse_template()
        deployment_properties = {
            'mode': DeploymentMode.incremental,
            'template': template_data
        }

        deployresp = self.resource_client.deployments.create_or_update(
            self.RESOURCE_GROUP_NAME,
            deployment_name,
            deployment_properties
        )
        deployresp.wait()
        print("ARM Template deployed", deployresp.status())

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
