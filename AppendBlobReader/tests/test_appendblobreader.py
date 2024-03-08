import os
from datetime import datetime
import time
import unittest
import json
import uuid
from time import sleep
from baseappendblobtest import BaseAppendBlobTest
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import BlobBlock
from azure.mgmt.storage import StorageManagementClient
from azure.cosmosdb.table.tableservice import TableService

class TestAppendBlobReader(BaseAppendBlobTest):
    
    
    @classmethod
    def setUpClass(self):
        current_time = datetime.now()
        datetime_value = current_time.strftime("%d-%m-%y-%H-%M-%S")
        self.collector_name = "azure_appendblob_unittest-%s" % (datetime_value)
        self.source_name = "appendblob_data-%s" % (datetime_value)
        super(TestAppendBlobReader, self).setUpClass()

         # create new test resource group and test storage account
        test_datetime_value = current_time.strftime("%d%m%y%H%M%S")
        self.test_storage_res_group = "testsumosa%s" % (test_datetime_value)
        self.test_storageaccount_name = "testsa%s" % (test_datetime_value)
        self.test_storageAccountRegion = "Central US"
        self.test_container_name = "testcontainer-%s" % (datetime_value)
        #self.test_filename = "testblob"
        #self.event_subscription_name = "SUMOBRSubscription"
        
        self.create_storage_account(self.test_storageAccountRegion,
                                    self.test_storage_res_group, self.test_storageaccount_name)
        self.block_blob_service = self.get_blockblob_service(
            self.test_storage_res_group, self.test_storageaccount_name)
        self.create_container(self.test_container_name)

        # resource group
        self.resource_group_name = "TABR-%s" % (datetime_value)
        self.template_name = 'appendblobreader.json'
        self.offsetmap_table_name = "FileOffsetMap"
        self.function_name = "BlobTaskConsumer"

        self.create_resource_group(
            self.resourcegroup_location, self.resource_group_name)

    def test_01_pipeline(self):
        self.deploy_template()
        self.assertTrue(self.resource_group_exists(self.resource_group_name))
        self.table_service = self.get_table_service()
        self.create_offset_table(self.offsetmap_table_name)
    
    @classmethod
    def get_blockblob_service(cls, resource_group, account_name):
        storage_client = StorageManagementClient(cls.azure_credential,
                                                 cls.subscription_id)
        storage_keys = storage_client.storage_accounts.list_keys(
            resource_group, account_name)
        acckey = storage_keys.keys[0].value
        block_blob_service = BlockBlobService(account_name=account_name,
                                              account_key=acckey)
        return block_blob_service

    def get_table_service(self):
        storage_client = StorageManagementClient(self.azure_credential,
                                                 self.subscription_id)
        STORAGE_ACCOUNT_NAME = self.get_resource_name(
            "sumobrlogs", "Microsoft.Storage/storageAccounts")
        storage_keys = storage_client.storage_accounts.list_keys(
            self.resource_group_name, STORAGE_ACCOUNT_NAME)
        acckey = storage_keys.keys[0].value
        table_service = TableService(account_name=STORAGE_ACCOUNT_NAME,
                                     account_key=acckey)
        return table_service
    
    @classmethod
    def create_container(cls, test_container_name):
        if not cls.block_blob_service.exists(test_container_name):
            cls.block_blob_service.create_container(test_container_name)
            cls.logger.info("creating container %s" % test_container_name)

    def create_offset_table(self, offsetmap_table_name):
        self.logger.info("creating FileOffsetMap table")
        self.table_service.create_table(offsetmap_table_name)

    def test_02_resource_count(self):
        expected_resource_count = 10
        self.check_resource_count(expected_resource_count)

if __name__ == '__main__':
    unittest.main()