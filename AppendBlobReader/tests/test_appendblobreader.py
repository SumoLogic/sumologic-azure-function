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
    def setUpClass(cls):
        current_time = datetime.now()
        datetime_value = current_time.strftime("%d-%m-%y-%H-%M-%S")
        cls.collector_name = "azure_appendblob_unittest-%s" % (datetime_value)
        cls.source_name = "appendblob_data-%s" % (datetime_value)
        cls.source_category = "azure_appendblob_logs-%s" % (datetime_value)
        super(TestAppendBlobReader, cls).setUpClass()

        # create new test resource group and test storage account
        test_datetime_value = current_time.strftime("%d%m%y%H%M%S")
        cls.test_storage_res_group = "testsumosarg%s" % (test_datetime_value)
        cls.test_storageaccount_name = "testsa%s" % (test_datetime_value)
        cls.test_storageAccountRegion = "Central US"
        cls.test_container_name = "testcontainer-%s" % (datetime_value)
        cls.test_filename = "test.blob"
        cls.event_subscription_name = "SUMOBRSubscription"

        cls.create_storage_account(cls.test_storageAccountRegion, cls.test_storage_res_group, cls.test_storageaccount_name)
        cls.block_blob_service = cls.get_blockblob_service( cls.test_storage_res_group, cls.test_storageaccount_name)
        cls.create_container(cls.test_container_name)

        # resource group
        cls.resource_group_name = "TABR-%s" % (datetime_value)
        cls.template_name = os.environ.get("TEMPLATE_NAME", "appendblobreaderdeploy.json")
        cls.offsetmap_table_name = "FileOffsetMap"

        cls.create_resource_group(cls.resourcegroup_location, cls.resource_group_name)

    def test_01_pipeline(self):
        self.deploy_template()
        self.assertTrue(self.resource_group_exists(self.resource_group_name))
        self.table_service = self.get_table_service()
        self.create_offset_table(self.offsetmap_table_name)

    def test_02_resource_count(self):
        expected_resource_count = 12  # 10 + 2(microsoft.insights/autoscalesettings)
        self.check_resource_count(expected_resource_count)

    def test_03_func_logs(self):
        self.logger.info("inserting mock data in BlobStorage")
        self.upload_file_chunks_using_append_blobs()

        time.sleep(600)

        app_insights = self.get_resource('Microsoft.Insights/components')

        # Azure function: AppendBlobFileTracker
        azurefunction = "AppendBlobFileTracker"
        captured_output = self.fetchlogs(app_insights.name, azurefunction)

        message = "Append blob scenario create just an entry RowKey:"
        self.assertTrue(self.filter_logs(captured_output, 'message', message),
                        f"No '{message}' log line found in '{azurefunction}' function logs")
        expected_count = 1
        record_count = self.filter_log_Count(captured_output, 'message', message)
        self.assertTrue(record_count == expected_count,
                        f"'{message}' log line count: {record_count} differs from expected count {expected_count} in '{azurefunction}' function logs")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '3'),
                        f"Error messages found in '{azurefunction}' logs: {captured_output}")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '2'),
                        f"Warning messages found in '{azurefunction}' logs: {captured_output}")

        # Azure function: AppendBlobTaskProducer
        azurefunction = "AppendBlobTaskProducer"
        captured_output = self.fetchlogs(app_insights.name, azurefunction)

        message = "New File Tasks created: 1 AppendBlob Archived Files: 0"
        self.assertTrue(self.filter_logs(captured_output, 'message', message),
                        f"No '{message}' log line found in '{azurefunction}' function logs")
        message = "BatchUpdateResults -  [ [ { status: 'success' } ], [] ]"
        self.assertTrue(self.filter_logs(captured_output, 'message', message),
                        f"No '{message}' log line found in '{azurefunction}' function logs")

        expected_count = 0
        message = "BatchUpdateResults -  [ [ { status: 'fail' } ], [] ]"
        record_count = self.filter_log_Count(
            captured_output, 'message', message)
        self.assertTrue(record_count == expected_count,
                        f"'{message}' log line count: {record_count} differs from expected count {expected_count} in '{azurefunction}' function logs")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '3'),
                        f"Error messages found in '{azurefunction}' logs: {captured_output}")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '2'),
                        f"Warning messages found in '{azurefunction}' logs: {captured_output}")

        # Azure function: AppendBlobTaskConsumer
        azurefunction = "AppendBlobTaskConsumer"
        captured_output = self.fetchlogs(app_insights.name, azurefunction)

        message = "All chunks successfully sent to sumo"
        self.assertTrue(self.filter_logs(captured_output, 'message', message),
                        f"No '{message}' log line found in '{azurefunction}' function logs")

        message = "Update offset result"
        self.assertTrue(self.filter_logs(captured_output, 'message', message),
                        f"No '{message}' log line found in '{azurefunction}' function logs")

        message = "Offset is already at the end"
        self.assertTrue(self.filter_logs(captured_output, 'message', message),
                        f"No '{message}' log line found in '{azurefunction}' function logs")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '3'),
                        f"Error messages found in '{azurefunction}' logs: {captured_output}")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '2'),
                        f"Warning messages found in '{azurefunction}' logs: {captured_output}")

    def test_04_sumo_query_record_count(self):
        self.logger.info("fetching mock data count from sumo")
        query = f'_sourceCategory="{self.source_category}" | count'
        relative_time_in_minutes = 30
        expected_record_count = 32768
        result = self.fetch_sumo_query_results(query, relative_time_in_minutes)
        #sample: {'warning': '', 'fields': [{'name': '_count', 'fieldType': 'int', 'keyField': False}], 'records': [{'map': {'_count': '32768'}}]}
        try:
            record_count = int(result['records'][0]['map']['_count'])
        except Exception:
            record_count = 0
        self.assertTrue(record_count == expected_record_count,
                        f"append blob file's record count: {record_count} differs from expected count {expected_record_count} in sumo '{self.source_category}'")

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
            "sumoablogs", "Microsoft.Storage/storageAccounts")
        storage_keys = storage_client.storage_accounts.list_keys(
            self.resource_group_name, STORAGE_ACCOUNT_NAME)
        acckey = storage_keys.keys[0].value
        table_service = TableService(account_name=STORAGE_ACCOUNT_NAME,
                                     account_key=acckey)
        return table_service

    def create_offset_table(self, offsetmap_table_name):
        self.logger.info("creating FileOffsetMap table")
        table_created = self.table_service.create_table(offsetmap_table_name)
        sleep(10)
        self.assertTrue(table_created, "Failed to create table")

    @classmethod
    def create_container(cls, test_container_name):
        if not cls.block_blob_service.exists(test_container_name):
            cls.block_blob_service.create_container(test_container_name)
            cls.logger.info("creating container %s" % test_container_name)

    def upload_file_chunks_using_append_blobs(self):

        current_file_size = 0
        log_line_num = 0
        self.block_blob_service.create_blob(
            self.test_container_name, self.test_filename)
        self.logger.info(
            f"Creating new file storage: {self.test_storageaccount_name}, container: {self.test_container_name}, blob: {self.test_filename}, current_file_size (in MB): 0, log_line_num: 0")

        logline = '''{ "time": "TIMESTAMP", "resourceId": "/SUBSCRIPTIONS/C088AD2A/RESOURCEGROUPS/SUMOAUDITCOLLECTION/PROVIDERS/MICROSOFT.WEB/SITES/HIMTEST", "operationName": "Microsoft.Web/sites/log", "category": "AppServiceConsoleLogs", "resultDescription": "000000000 WARNING:root:testing warn level\\n\\n", "level": "Error", "EventStampType": "Stamp", "EventPrimaryStampName": "waws-prod-blu-161", "EventStampName": "waws-prod-blu-161h", "Host": "RD501AC57BA3D4", "LineNo": LINENUM}'''

        for i in range(4):
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
                f"current_chunk_size (in MB): {cur_size/(1024*1024)} log_line_num: {log_line_num} current_file_size: {current_file_size/(1024*1024)}")
            self.block_blob_service.append_blob_from_text(self.test_container_name, self.test_filename, chunk, encoding='utf-8')
            time.sleep(120)

        self.logger.info(
            f"Finished uploading current_file_size (in MB): {current_file_size/(1024*1024)} last_log_line_num: {log_line_num} storage: {self.test_storageaccount_name} container: {self.test_container_name} blob: {self.test_filename} ")


if __name__ == '__main__':
    unittest.main()
