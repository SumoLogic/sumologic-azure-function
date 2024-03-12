import os
from datetime import datetime
import time
import unittest
import json
import uuid
from time import sleep
from baseappendblobtest import BaseAppendBlobTest
from azure.storage.blob import AppendBlobService
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
        self.test_filename = "testblob"
        self.event_subscription_name = "SUMOBRSubscription"
        
        self.create_storage_account(self.test_storageAccountRegion, self.test_storage_res_group, self.test_storageaccount_name)
        self.block_blob_service = self.get_blockblob_service( self.test_storage_res_group, self.test_storageaccount_name)
        self.create_container(self.test_container_name)

        # resource group
        self.resource_group_name = "TABR-%s" % (datetime_value)
        self.template_name = 'appendblobreader.json'
        self.offsetmap_table_name = "FileOffsetMap"
        self.function_name = "BlobTaskConsumer"

        self.create_resource_group(self.resourcegroup_location, self.resource_group_name)

    def test_01_pipeline(self):
        self.deploy_template()
        self.assertTrue(self.resource_group_exists(self.resource_group_name))
        self.table_service = self.get_table_service()
        self.create_offset_table(self.offsetmap_table_name)

    def test_02_resource_count(self):
        expected_resource_count = 12 # 10 + 2(microsoft.insights/autoscalesettings)
        self.check_resource_count(expected_resource_count)
    
    def test_03_insert_chunks(self):
        self.logger.info("inserting mock data in BlobStorage")
        self.upload_file_chunks_using_append_blobs()

        time.sleep(1200)

        app_insights = self.get_resource('Microsoft.Insights/components')
        captured_output = self.fetchlogs(app_insights.name)

        successful_sent_message = "All chunks successfully sent to sumo"
        self.assertTrue(self.filter_logs(captured_output, 'message', successful_sent_message),
                        "No success message found in azure function logs")

        end_message = "Offset is already at the end"
        self.assertTrue(self.filter_logs(captured_output, 'message', end_message),
                        "No success message found in azure function logs")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '3'),
                         "Error messages found in azure function logs")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '2'),
                         "Warning messages found in azure function logs")
    
    @classmethod
    def get_blockblob_service(cls, resource_group, account_name):
        storage_client = StorageManagementClient(cls.azure_credential,
                                                 cls.subscription_id)
        storage_keys = storage_client.storage_accounts.list_keys(
            resource_group, account_name)
        acckey = storage_keys.keys[0].value
        block_blob_service = AppendBlobService(
            account_name=account_name, account_key=acckey)
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

    def create_offset_table(self, offsetmap_table_name):
        self.logger.info("creating FileOffsetMap table")
        self.table_service.create_table(offsetmap_table_name)
    
    @classmethod
    def create_container(cls, test_container_name):
        if not cls.block_blob_service.exists(test_container_name):
            cls.block_blob_service.create_container(test_container_name)
            cls.logger.info("creating container %s" % test_container_name)

    def getLastLogLineNumber(self, current_file_size):
        if current_file_size <= 0:
            return 0
        offset = max(current_file_size-500, 0)
        blob_data = self.block_blob_service.get_blob_to_text(self.test_container_name, self.test_filename, encoding='utf-8',
                                                             start_range=offset)
        self.logger.info(f"data: {blob_data.content}")
        data = blob_data.content
        line_num = data.rsplit("LineNo\":", 1)[-1].rstrip('}\n').strip()
        return int(line_num)

    def upload_file_chunks_using_append_blobs(self):

        max_file_size = 4*1024*1024
        if not self.block_blob_service.exists(self.test_container_name, self.test_filename):
            self.logger.info(f"blob not exists in container {self.test_container_name} {self.test_filename}")
            self.block_blob_service.create_blob(self.test_container_name, self.test_filename)
            current_file_size = 0
            self.logger.info(
                f"Creating new file storage: {self.test_storageaccount_name} container: {self.test_container_name} blob: {self.test_filename} ")
            log_line_num = 0
        else:
            current_file_size = self.block_blob_service.get_blob_properties(
                self.test_container_name, self.test_filename).properties.content_length
            log_line_num = self.getLastLogLineNumber(current_file_size)

        self.logger.info(
            f"current_file_size (in MB): {current_file_size/(1024*1024)} log_line_num: {log_line_num} storage: {self.test_storageaccount_name} container: {self.test_container_name} blob: {self.test_filename} ")
        logline = '''{ "time": "TIMESTAMP", "resourceId": "/SUBSCRIPTIONS/C088AD2A/RESOURCEGROUPS/SUMOAUDITCOLLECTION/PROVIDERS/MICROSOFT.WEB/SITES/HIMTEST", "operationName": "Microsoft.Web/sites/log", "category": "AppServiceConsoleLogs", "resultDescription": "000000000 WARNING:root:testing warn level\\n\\n", "level": "Error", "EventStampType": "Stamp", "EventPrimaryStampName": "waws-prod-blu-161", "EventStampName": "waws-prod-blu-161h", "Host": "RD501AC57BA3D4", "LineNo": LINENUM}'''

        while current_file_size < max_file_size:
            # since (4*1024*1024)/512(size of logline) = 8192
            msg = []
            for idx in range(8192):
                log_line_num += 1
                current_datetime = datetime.now().isoformat()
                cur_msg = logline.replace("TIMESTAMP", current_datetime)
                cur_msg = cur_msg.replace("LINENUM", f'{log_line_num:10d}')
                msg.append(cur_msg)

            chunk = "\n".join(msg) + "\n"
            cur_size = len(chunk.encode('utf-8')) 
            current_file_size += cur_size
            self.logger.info(
                f"current_chunk_size (in MB): {cur_size/(1024*1024)} log_line_num: {log_line_num} current_file_size: {current_file_size/(1024*1024)} storage: {self.test_storageaccount_name} container: {self.test_container_name} blob: {self.test_filename} ")
            self.block_blob_service.append_blob_from_text(self.test_container_name, self.test_filename, chunk, encoding='utf-8')
                # time.sleep(20)

        self.logger.info(
            f"Finished uploading current_file_size (in MB): {current_file_size/(1024*1024)} last_log_line_num: {log_line_num} storage: {self.test_storageaccount_name} container: {self.test_container_name} blob: {self.test_filename} ")


if __name__ == '__main__':
    unittest.main()