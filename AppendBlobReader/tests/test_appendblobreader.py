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
from random import choices
from string import ascii_uppercase, digits


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
        # Verify when Test Storage Account and template deployment are in different regions
        cls.test_storageAccountRegion = "Central US"
        cls.test_container_name = "testcontainer-%s" % (datetime_value)
        # https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
        # https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules
        # storageAccount 3-24 container 3-63 blobName 1-1024 maxDepth 63 if hierarchial namespace enabled
        cls.bigrandomfilename = str.join('', choices(ascii_uppercase+digits, k=1015))
        folder_depth = str.join('/', choices(ascii_uppercase+digits, k=62))
        cls.test_filename = f"{folder_depth}/test{cls.bigrandomfilename}.blob"
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
        self.create_offset_table(self.offsetmap_table_name)  # now this gets created automatically

    def test_02_resource_count(self):
        expected_resource_count = 12  # 10 + 2(microsoft.insights/autoscalesettings)
        self.check_resource_count(expected_resource_count)

    def upload_file_in_another_container(self):
        self.logger.info("uploading file in another container outside filter prefix")
        test_container_name_excluded_by_filter = "anothercontainernotinprefix"
        cls.test_filename_excluded_by_filter = "test_filename_excluded_by_filter.blob"
        line_not_present = "this line should not be present"
        chunk = "\n".join(line_not_present) + "\n"
        self.create_container(test_container_name_excluded_by_filter)
        self.block_blob_service.create_blob(test_container_name_excluded_by_filter, cls.test_filename_excluded_by_filter)
        self.block_blob_service.append_blob_from_text(test_container_name_excluded_by_filter, cls.test_filename_excluded_by_filter, chunk, encoding='utf-8')

    def upload_file_of_unknown_extension(self):
        self.logger.info("uploading file with unsupported extension")
        cls.test_filename_unsupported_extension = "test.xml"
        line_not_present = '<?xml version="1.0"?>'
        chunk = "\n".join(line_not_present) + "\n"
        self.create_container(self.test_container_name)
        self.block_blob_service.create_blob(self.test_container_name, cls.test_filename_unsupported_extension)
        self.block_blob_service.append_blob_from_text(self.test_container_name, cls.test_filename_unsupported_extension, chunk, encoding='utf-8')

    def test_03_func_logs(self):
        self.logger.info("inserting mock data in BlobStorage")
        self.upload_file_chunks_using_append_blobs()
        self.upload_file_in_another_container()
        self.upload_file_of_unknown_extension()
        time.sleep(600)

        app_insights = self.get_resource('Microsoft.Insights/components')

        # Azure function: AppendBlobFileTracker
        azurefunction = "AppendBlobFileTracker"
        captured_output = self.fetchlogs(app_insights.name, azurefunction)

        message = "FileOffSetMap Rows Created: 1 Existing Rows: 0 Failed: 0"
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

        # Verify Debug Logs are created in Storage account for successful upload
        message = "All chunks successfully sent to sumo"
        self.assertTrue(self.filter_logs(captured_output, 'message', message),
                        f"No '{message}' log line found in '{azurefunction}' function logs")

        message = "Updated offset result"
        self.assertTrue(self.filter_logs(captured_output, 'message', message),
                        f"No '{message}' log line found in '{azurefunction}' function logs")

        # Invalid Range when offset is more than the blob size
        message = "Offset is already at the end"
        self.assertTrue(self.filter_logs(captured_output, 'message', message),
                        f"No '{message}' log line found in '{azurefunction}' function logs")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '3'),
                        f"Error messages found in '{azurefunction}' logs: {captured_output}")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '2'),
                        f"Warning messages found in '{azurefunction}' logs: {captured_output}")

    def test_04_sumo_query_record_count(self):
        self.logger.info("fetching mock data count from sumo")
        query = f'_sourceCategory="{self.source_category}" | count by _sourceName, _sourceHost'
        relative_time_in_minutes = 30
        expected_record_count = 32768
        record_count = record_excluded_by_filter_count = 0
        source_host = source_name = ""
        #sample: {'warning': '', 'fields': [{'name': '_count', 'fieldType': 'int', 'keyField': False}], 'records': [{'map': {'_count': '32768'}}]}
        try:
            result = self.fetch_sumo_query_results(query, relative_time_in_minutes)
            record_count = int(result['records'][0]['map']['_count'])
            source_name = int(result['records'][0]['map']['_sourceName'])
            source_host = int(result['records'][0]['map']['_sourceHost'])
            record_excluded_by_filter_count = self.fetch_sumo_query_results(f'_sourceName="{self.test_filename_excluded_by_filter}" | count', relative_time_in_minutes)['records'][0]['map']['_count']
            record_unsupported_extension_count = self.fetch_sumo_query_results(f'_sourceName="{self.test_filename_unsupported_extension}" | count', relative_time_in_minutes)['records'][0]['map']['_count']
        except Exception as err:
            self.logger.info(f"Error in fetching sumo query results {err}")

        # File should be successfully uploaded in Sumo logic without duplication
        self.assertTrue(record_count == expected_record_count,
                        f"append blob file's record count: {record_count} differs from expected count {expected_record_count} in sumo '{self.source_category}'")
        # Verify Filter Prefix field
        self.assertTrue(record_excluded_by_filter_count == 0,
                        f"append blob file's record count: {record_excluded_by_filter_count}, logs outside container filter prefix should not be ingested")
        # Verify unsupported file type (done)
        self.assertTrue(record_unsupported_extension_count == 0,
                        f"append blob file's record count: {record_unsupported_extension_count}, logs with unsupported blob extension should not be ingested")

        # Verify with a very long append blob filename (1024 characters) (done)
        if len(self.test_filename) > 128:
            expected_filename = self.test_filename[:60] + "..." + self.test_filename[-60:]
        else:
            expected_filename = self.test_filename

        # Verify addition of _sourceCategory, _sourceHost, _sourceName and also additional metadata (done)
        self.assertTrue(source_name == f"{expected_filename}", f"_sourceName {source_name} metadata is incorrect")
        self.assertTrue(source_host == f"{self.test_storageaccount_name}/{self.test_container_name}", f"_sourceHost {source_host} metadata is incorrect")

    # def test_05_upload_filename_with_utf16_chars_having_utf16_chars_in_deep_folder():
    #     # Verify with a filename with special characters
    #     # Verify maximum length path  of storage location
    #     # Todo find out how to upload file with a prefix
    #     self.logger.info("uploading file with non ascii chars and filename")
    #     cls.test_filename_nonascii_chars = "nonascii.txt"
    #     log_line = "log line with non ascii chars 汉字"
    #     chunk = "\n".join(line_not_present) + "\n"
    #     self.create_container(self.test_container_name)
    #     self.block_blob_service.create_blob(test_container_name, cls.test_filename_nonascii_chars)
    #     self.block_blob_service.append_blob_from_text(test_container_name, test_filename_nonascii_chars, chunk, encoding='utf-16')
    #
    #
    # def test_06_deleted_file_entry_should_be_removed():
    #     # delete the file
    #     # trigger the producer function
    #     # check the consumer logs

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
