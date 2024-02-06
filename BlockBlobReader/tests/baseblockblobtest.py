import json
import os
import sys

from azure.mgmt.storage import StorageManagementClient

sys.path.insert(0, '../../test_utils')
from basetest import BaseTest


class BaseBlockBlobTest(BaseTest):
  
    def tearDown(self):
        self.delete_container()
        
        if self.resource_group_exists(self.resource_group_name):
            self.delete_resource_group(self.resource_group_name)
        
        if self.resource_group_exists(self.test_storage_res_group):
            self.delete_resource_group(self.test_storage_res_group)

        self.delete_source(self.collector_id, self.sumo_source_id)
        self.delete_collector(self.collector_id)
        self.sumologic_cli.session.close()
    
    def _parse_template(self):
        template_path = os.path.join(os.path.abspath('..'), 'src',
                                     self.template_name)

        print("Reading template from %s" % template_path)
        with open(template_path, 'r') as template_file_fd:
            template_data = json.load(template_file_fd)

        template_data["parameters"]["SumoEndpointURL"]["defaultValue"] = self.sumo_endpoint_url
        template_data["parameters"]["sourceCodeBranch"]["defaultValue"] = self.branch_name
        template_data["parameters"]["sourceCodeRepositoryURL"]["defaultValue"] = self.repo_name
        template_data["parameters"]["StorageAccountName"]["defaultValue"] = self.test_storageaccount_name
        template_data["parameters"]["StorageAccountResourceGroupName"]["defaultValue"] = self.test_storage_res_group
        template_data["parameters"]["StorageAccountRegion"]["defaultValue"] = self.test_storageAccountRegion
        template_data["parameters"]["location"]["defaultValue"] = self.resourcegroup_location
        return template_data
    
    def create_storage_account(self, location, resource_group_name, storageaccount_name):
        # Step 1: Provision the resource group.
        print(f"Creating ResourceGroup for StorageAccount {resource_group_name}")
        self.create_resource_group(location, resource_group_name)
        
        # Step 2: Provision the storage account, starting with a management object.
        storage_client = StorageManagementClient(self.azure_credential, self.subscription_id)

        # The name is available, so provision the account
        account = storage_client.storage_accounts.begin_create(resource_group_name, storageaccount_name,
                                                            {
                                                                "location": location,
                                                                "kind": "StorageV2",
                                                                "sku": {"name": "Standard_LRS"}
                                                            }
                                                            )

        account_result = account.result()
        print(f"Storage account '{account_result.name}' created")

