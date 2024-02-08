import os
from datetime import datetime
import time
import unittest
import json
import uuid
from time import sleep
from baseblockblobtest import BaseBlockBlobTest
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import BlobBlock
from azure.mgmt.storage import StorageManagementClient
from azure.cosmosdb.table.tableservice import TableService

class TestBlobReaderFlow(BaseBlockBlobTest):

    @classmethod
    def setUpClass(self):
        current_time = datetime.now()
        datetime_value = current_time.strftime("%d-%m-%y-%H-%M-%S")
        self.collector_name = "azure_blob_unittest-%s" % (datetime_value)
        self.source_name = "blob_data-%s" % (datetime_value)
        super(TestBlobReaderFlow, self).setUpClass()

        # create new test resource group and test storage account
        test_datetime_value = current_time.strftime("%d%m%y%H%M%S")
        self.test_storage_res_group = "sumosa%s" % (test_datetime_value)
        self.test_storageaccount_name = "sa%s" % (test_datetime_value)
        self.test_storageAccountRegion = "Central US"
        self.test_container_name = "testcontainer-%s" % (datetime_value)
        self.test_filename = "testblob"
        self.event_subscription_name = "SUMOBRSubscription"
        
        self.create_storage_account(self.test_storageAccountRegion,
                                    self.test_storage_res_group, self.test_storageaccount_name)
        self.block_blob_service = self.get_blockblob_service(
            self.test_storage_res_group, self.test_storageaccount_name)
        self.create_container(self.test_container_name)

        # resource group
        self.resource_group_name = "TBL-%s" % (datetime_value)
        self.template_name = 'blobreaderdeploy.json'
        self.log_table_name = "AzureWebJobsHostLogs%d%02d" % (
            datetime.now().year, datetime.now().month)
        self.offsetmap_table_name = "FileOffsetMap"
        self.function_name = "BlobTaskConsumer"

        self.create_resource_group(
            self.resourcegroup_location, self.resource_group_name)

    def test_01_pipeline(self):
        self.logger.info("started: test_01_pipeline")
        self.deploy_template()
        self.assertTrue(self.resource_group_exists(self.resource_group_name))
        self.table_service = self.get_table_service()
        self.create_offset_table(self.offsetmap_table_name)
        self.logger.info("ended: test_01_pipeline")

    def test_02_resource_count(self):
        self.logger.info("started: test_02_resource_count")
        expected_resource_count = 10
        self.check_resource_count(expected_resource_count)
        self.logger.info("ended: test_02_resource_count")

    def test_03_func_logs(self):
        self.logger.info("started: test_03_func_logs")
        log_type = os.environ.get("LOG_TYPE", "log")
        self.logger.info("inserting mock %s data in BlobStorage" % log_type)
        if log_type in ("csv", "log",  "blob"):
            self.insert_mock_logs_in_BlobStorage(log_type)
        else:
            self.insert_mock_json_in_BlobStorage()
        
        time.sleep(300)
        app_insights = self.get_resource('Microsoft.Insights/components')
        captured_output = self.fetchlogs(app_insights.name)

        successful_sent_message = "Successfully sent to Sumo, Exiting now."
        self.assertTrue(self.filter_logs(captured_output, 'message', successful_sent_message),
                        "No success message found in azure function logs")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '3'),
                         "Error messages found in azure function logs")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '2'),
                         "Warning messages found in azure function logs")
        self.logger.info("ended: test_03_func_logs")


    # def check_both_storage_accounts_present():
    #     pass

    # def check_sorted_task_range():
    #     pass

    # def check_offset_in_range():
    #     pass

    # def extract_tasks_from_logs():
    #     pass

    # def check_one_to_one_task_mapping():
    #     pass

    def get_random_name(self, length=32):
        return str(uuid.uuid4())

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
        # table = table_service.list_tables().items[0]  # flaky
        return table_service

    def create_offset_table(self, offsetmap_table_name):
        self.logger.info("creating FileOffsetMap table")
        self.table_service.create_table(offsetmap_table_name)

    def create_test_storage_account():
        # Todo: remove storage account allbloblogs dependency
        pass
    
    @classmethod
    def create_container(cls, test_container_name):
        if not cls.block_blob_service.exists(test_container_name):
            cls.block_blob_service.create_container(test_container_name)
            cls.logger.info("creating container %s" % test_container_name)

    # def delete_container(self):
    #     if self.block_blob_service.exists(self.test_container_name):
    #         self.block_blob_service.delete_container(self.test_container_name)
    #         self.logger.info("deleting container %s" % self.test_container_name)

    def create_or_update_blockblob(self, container_name, file_name, datalist, blocks):
        block_id = self.get_random_name()
        file_bytes = ''.join(datalist).encode()
        self.block_blob_service.put_block(
            container_name, file_name, file_bytes, block_id)
        blocks.append(BlobBlock(id=block_id))
        self.block_blob_service.put_block_list(
            container_name, file_name, blocks)
        return blocks

    def get_current_blocks(self, test_container_name, test_filename):
        blocks = []
        if self.block_blob_service.exists(test_container_name,
                                          test_filename):
            blockslist = self.block_blob_service.get_block_list(
                test_container_name, test_filename, None, 'all')
            for block in blockslist.committed_blocks:
                blocks.append(BlobBlock(id=block.id))
        return blocks

    def get_json_data(self):
        json_data = json.load(open("blob_fixtures.json"))["records"]
        return [json_data[:2], json_data[2:5], json_data[5:7], json_data[7:]]

    def insert_empty_json(self, container_name, file_name):
        json_data = ['{"records":[', ']}']
        blocks = []
        for file_bytes in json_data:
            file_bytes = file_bytes.encode()
            block_id = self.get_random_name()
            self.block_blob_service.put_block(
                container_name, file_name, file_bytes, block_id)
            blocks.append(BlobBlock(id=block_id))
            self.block_blob_service.put_block_list(
                container_name, file_name, blocks)
        return blocks

    def insert_mock_json_in_BlobStorage(self):
        test_filename = self.test_filename + ".json"
        # Todo refactor this to get current blocks
        blocks = self.insert_empty_json(
            self.test_container_name, test_filename)
        for i, data_block in enumerate(self.get_json_data()):
            block_id = self.get_random_name()
            file_bytes = json.dumps(data_block)
            file_bytes = (file_bytes[1:-1] if i ==
                          0 else "," + file_bytes[1:-1]).encode()
            self.block_blob_service.put_block(
                self.test_container_name, test_filename, file_bytes, block_id)
            blocks.insert(len(blocks)-1, BlobBlock(id=block_id))
            self.block_blob_service.put_block_list(
                self.test_container_name, test_filename, blocks)
        self.logger.info("inserted %s" % (blocks))

    def get_csv_data(self):
        all_lines = []
        with open("blob_fixtures.csv") as logfile:
            all_lines = logfile.readlines()
        return [all_lines[:2], all_lines[2:5], all_lines[5:7]] + self.get_chunks(all_lines[7:], 2)

    def get_chunks(self, l, s):
        return [l[i:i+s] for i in range(0, len(l), s)]

    def get_log_data(self):
        all_lines = []
        with open("blob_fixtures.log") as logfile:
            all_lines = logfile.readlines()
        return [all_lines[:2], all_lines[2:5], all_lines[5:7], all_lines[7:]]

    def get_blob_formatted_data(self):
        all_lines = []
        with open("blob_fixtures.blob") as logfile:
            all_lines = logfile.readlines()
        return [all_lines[:2], all_lines[2:5], all_lines[5:7]] + self.get_chunks(all_lines[7:], 2)

    def insert_mock_logs_in_BlobStorage(self, file_ext):
        blocks = []
        datahandler = {'log': 'get_log_data',
                       'csv': 'get_csv_data', 'blob': 'get_blob_formatted_data'}
        test_filename = self.test_filename + "." + file_ext
        blocks = self.get_current_blocks(
            self.test_container_name, test_filename)
        for data_block in getattr(self, datahandler.get(file_ext))():
            blocks = self.create_or_update_blockblob(self.test_container_name,
                                                     test_filename,
                                                     data_block, blocks)

        self.logger.info("inserted %s" % (blocks))

if __name__ == '__main__':
    unittest.main()
