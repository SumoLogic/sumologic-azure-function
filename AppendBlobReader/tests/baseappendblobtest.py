import json
import os
import sys

from azure.mgmt.storage import StorageManagementClient

sys.path.insert(0, '../../test_utils')
from basetest import BaseTest


class BaseAppendBlobTest(BaseTest):

    @classmethod
    def tearDownClass(cls):
        super(BaseAppendBlobTest, cls).tearDownClass()
        if cls.resource_group_exists(cls.test_storage_res_group):
            cls.delete_resource_group(cls.test_storage_res_group)

    def _parse_template(self):
        template_path = os.path.join(os.path.abspath('..'), 'src',
                                     self.template_name)

        self.logger.info("reading template from %s" % template_path)
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

    @classmethod
    def create_storage_account(cls, location, resource_group_name, storageaccount_name):
        # Step 1: Provision the resource group.
        cls.logger.info(
            f"creating ResourceGroup for StorageAccount: {resource_group_name}")
        cls.create_resource_group(location, resource_group_name)

        # Step 2: Provision the storage account, starting with a management object.
        storage_client = StorageManagementClient(
            cls.azure_credential, cls.subscription_id)

        # The name is available, so provision the account
        account = storage_client.storage_accounts.begin_create(
            resource_group_name, storageaccount_name, {"location": location, "kind": "StorageV2", "sku": {"name": "Standard_LRS"}})

        account_result = account.result()
        cls.logger.info(f"created Storage account: {account_result.name}")
