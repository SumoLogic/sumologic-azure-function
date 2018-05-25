import unittest
import os
import json
from azure.common.credentials import ServicePrincipalCredentials


class BaseTest(unittest.TestCase):

    def create_credentials(self):
        config_file = os.path.expanduser("~/.azure/azure_credentials.json")
        if os.path.isfile(config_file):
            config = json.load(open(config_file))
        else:
            config = {
                'AZURE_SUBSCRIPTION_ID': os.environ['AZURE_SUBSCRIPTION_ID'],
                'AZURE_CLIENT_ID': os.environ['AZURE_CLIENT_ID'],
                'AZURE_CLIENT_SECRET': os.environ['AZURE_CLIENT_SECRET'],
                'AZURE_TENANT_ID': os.environ['AZURE_TENANT_ID'],
                'AZURE_DEFAULT_REGION': os.environ.get("AZURE_DEFAULT_REGION",
                                                       "westus")
            }
        self.subscription_id = str(config['AZURE_SUBSCRIPTION_ID'])

        self.credentials = ServicePrincipalCredentials(
            client_id=config['AZURE_CLIENT_ID'],
            secret=config['AZURE_CLIENT_SECRET'],
            tenant=config['AZURE_TENANT_ID']
        )
        self.location = str(config['AZURE_DEFAULT_REGION'])

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
