import os
from datetime import datetime, timedelta
import time
import unittest
import json
import uuid
from baseblockblobtest import BaseBlockBlobTest
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import BlobBlock
from azure.mgmt.storage import StorageManagementClient
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.mgmt.web import WebSiteManagementClient
from azure.cosmosdb.table.tableservice import TableService
from random import choices
from string import ascii_uppercase, digits


class TestBlobReaderFlow(BaseBlockBlobTest):

    @classmethod
    def setUpClass(cls):
        current_time = datetime.now()
        datetime_value = current_time.strftime("%d-%m-%y-%H-%M-%S")
        cls.collector_name = "azure_blockblob_unittest-%s" % (datetime_value)
        cls.source_name = "blockblob_data-%s" % (datetime_value)
        cls.source_category = "azure_blockblob_logs-%s" % (datetime_value)
        super(TestBlobReaderFlow, cls).setUpClass()

        # create new test resource group and test storage account
        test_datetime_value = current_time.strftime("%d%m%y%H%M%S")
        cls.test_storage_res_group = "testsumosa%s" % (test_datetime_value)
        cls.test_storageaccount_name = "testsa%s" % (test_datetime_value)
        # Verify when Test Storage Account and template deployment are in different regions
        cls.test_storageAccountRegion = "Central US"
        cls.FIXTURE_FILE = os.environ.get("FIXTURE_FILE", "blob_fixtures.json")
        cls.log_type = cls.FIXTURE_FILE.split(".")[-1]

        cls.test_container_name = cls.get_test_container_name()
        cls.test_filename_excluded_by_filter = "blockblob_test_filename_excluded_by_filter.blob"
        cls.test_filename_unsupported_extension = "blockblob_test.xml"
        # https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
        # https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules
        # storageAccount 3-24 container 3-63 blobName 1-1024 maxDepth 63 if hierarchial namespace enabled
        # Verify maximum length path of storage location
        cls.MAX_FOLDER_DEPTH = int(os.environ.get("MAX_FOLDER_DEPTH", 62))
        folder_depth = str.join('/', choices(ascii_uppercase+digits, k=cls.MAX_FOLDER_DEPTH))
        bigrandomfilename = str.join('', choices(ascii_uppercase+digits, k=(1024-10-len(folder_depth))))
        # extension is appended later
        cls.test_filename = f"{folder_depth}/test{bigrandomfilename}"
        cls.event_subscription_name = "SUMOBRSubscription"

        cls.create_storage_account(cls.test_storageAccountRegion,
                                    cls.test_storage_res_group, cls.test_storageaccount_name)
        cls.block_blob_service = cls.get_blockblob_service(
            cls.test_storage_res_group, cls.test_storageaccount_name)
        cls.create_container(cls.test_container_name)

        # resource group
        cls.resource_group_name = "TBL-%s" % (datetime_value)
        cls.template_name = os.environ.get("TEMPLATE_NAME", "blobreaderdeploy.json")
        cls.offsetmap_table_name = "FileOffsetMap"

        cls.create_resource_group(
            cls.resourcegroup_location, cls.resource_group_name)

    @classmethod
    def get_test_container_name(cls):
        if cls.log_type != "json":
            return "testcontainer-%s" % (datetime_value)
        elif cls.FIXTURE_FILE == "blob_fixtures.json":
            return "insights-logs-networksecuritygroupflowevent"
        else:
            return "insights-logs-flowlogflowevent"

    def test_01_pipeline(self):
        self.deploy_template()
        self.assertTrue(self.resource_group_exists(self.resource_group_name))
        self.table_service = self.get_table_service()
        self.create_offset_table(self.offsetmap_table_name)

    def test_02_resource_count(self):
        expected_resource_count = 10
        self.check_resource_count(expected_resource_count)

    def upload_file_in_another_container(self):
        self.logger.info("uploading file in another container outside filter prefix")
        test_container_name_excluded_by_filter = "anothercontainernotinprefix"
        line_not_present = "this line should not be present"
        data_block = [line_not_present]
        self.create_container(test_container_name_excluded_by_filter)
        blocks = self.create_or_update_blockblob(test_container_name_excluded_by_filter,
                                                     self.test_filename_excluded_by_filter,
                                                     data_block, [])

    def upload_file_of_unknown_extension(self):
        self.logger.info("uploading file with unsupported extension")
        line_not_present = '<?xml version="1.0"?>'
        data_block = [line_not_present]
        self.create_container(self.test_container_name)
        blocks = self.create_or_update_blockblob(self.test_container_name,
                                                     self.test_filename_unsupported_extension,
                                                     data_block, [])

    def get_full_testlog_file_name(self):
        # Verify with a very long append blob filename (1024 characters)
        file_ext = f".{self.log_type}"
        # _sourceCategory, _sourceHost, _sourceName have this limit
        maxMetadataLength = 1024
        if len(self.test_filename) > maxMetadataLength:
            expected_filename = self.test_filename[:60] + "..." + self.test_filename[-(60-len(file_ext)):] + file_ext
        else:
            expected_filename = self.test_filename + file_ext
        return expected_filename

    def test_03_func_logs(self):
        self.logger.info("inserting mock %s data in BlobStorage" % self.FIXTURE_FILE)
        if self.log_type in ("csv", "log",  "blob"):
            self.insert_mock_logs_in_BlobStorage(self.log_type)
        else:
            self.insert_mock_json_in_BlobStorage()

        self.upload_file_in_another_container()
        self.upload_file_of_unknown_extension()
        time.sleep(300)
        app_insights = self.get_resource('Microsoft.Insights/components')

        azurefunction = "BlobTaskProducer"
        captured_output = self.fetchlogs(app_insights.name, azurefunction)

        message = "Tasks Created:"
        self.assertTrue(self.filter_logs(captured_output, 'message', message),
                        f"No '{message}' log line found in '{azurefunction}' function logs")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '3'),
                        f"Error messages found in '{azurefunction}' logs: {captured_output}")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '2'),
                        f"Warning messages found in '{azurefunction}' logs: {captured_output}")

        azurefunction = "BlobTaskConsumer"
        captured_output = self.fetchlogs(app_insights.name, azurefunction)

        successful_sent_message = "Successfully sent to Sumo, Exiting now."
        self.assertTrue(self.filter_logs(captured_output, 'message', successful_sent_message),
                        f"No success message found in {azurefunction} azure function logs")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '3'),
                         f"Error messages found in {azurefunction} azure function logs:  {captured_output}")

        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '2'),
                         f"Warning messages found in {azurefunction} azure function logs:  {captured_output}")

        self.logger.info("fetching mock data count from sumo")

        query = f'_sourceCategory="{self.source_category}" | count by _sourceName, _sourceHost'
        relative_time_in_minutes = 30
        expected_record_count = {
            "blob_fixtures.blob": 15,
            "blob_fixtures.log": 10,
            "blob_fixtures.json": 153,
            "blob_fixtures_subnetflowlogs.json": 68,
            "blob_fixtures_vnetflowlogs.json": 166,
            "blob_fixtures_networkinterfaceflowlogs.json": 98,
            "blob_fixtures.csv": 12
        }
        record_count = record_excluded_by_filter_count = record_unsupported_extension_count = None
        source_host = source_name = ""
        #sample: {'warning': '', 'fields': [{'name': '_count', 'fieldType': 'int', 'keyField': False}], 'records': [{'map': {'_count': '10'}}]}
        try:
            result = self.fetch_sumo_query_results(query, relative_time_in_minutes)
            record_count = int(result['records'][0]['map']['_count'])
            source_name = result['records'][0]['map']['_sourcename']
            source_host = result['records'][0]['map']['_sourcehost']
            record_excluded_by_filter_count = len(self.fetch_sumo_query_results(f'_sourceName="{self.test_filename_excluded_by_filter}" | count', relative_time_in_minutes)['records'])
            record_unsupported_extension_count = len(self.fetch_sumo_query_results(f'_sourceName="{self.test_filename_unsupported_extension}" | count', relative_time_in_minutes)['records'])
        except Exception as err:
            self.logger.info(f"Error in fetching sumo query results {err}")

        self.assertTrue(record_count == expected_record_count.get(self.FIXTURE_FILE),
                        f"block blob file's record count: {record_count} differs from expected count {expected_record_count.get(self.FIXTURE_FILE)} in sumo '{self.source_category}'")

        # Verify Filter Prefix field
        self.assertTrue(record_excluded_by_filter_count == 0,
                        f"block blob file's record count: {record_excluded_by_filter_count}, logs outside container filter prefix should not be ingested")
        # Verify unsupported file type
        self.assertTrue(record_unsupported_extension_count == 0,
                        f"block blob file's record count: {record_unsupported_extension_count}, logs with unsupported blob extension should not be ingested")

        expected_filename = self.get_full_testlog_file_name()

        # Verify addition of _sourceCategory, _sourceHost, _sourceName and also additional metadata
        self.assertTrue(source_name == expected_filename, f"_sourceName: {source_name} expected_filename: {expected_filename} metadata is incorrect")
        self.assertTrue(source_host == f"{self.test_storageaccount_name}/{self.test_container_name}", f"_sourceHost {source_host} expected_sourcehost: {self.test_storageaccount_name}/{self.test_container_name} metadata is incorrect")

        # testing DLQ flow
        self.subtest_DLQ_func_logs()

    def upload_message_in_service_bus(self):
        file_ext = f".{self.log_type}"
        test_filename = self.test_filename + file_ext
        with open(self.FIXTURE_FILE, "r") as fp:
            file_size = len(fp.read())

        triggerData = {
            "blobName": test_filename,
            "containerName": self.test_container_name,
            "endByte": file_size-1,
            "resourceGroupName": self.test_storage_res_group,
            "startByte": 0,
            "storageName": self.test_storageaccount_name,
            "subscriptionId": self.subscription_id,
            "url": f"https:{self.test_storageaccount_name}.blob.core.windows.net/{self.test_container_name}/{test_filename}"
        }
        self.logger.info("Ingesting message into servicebus DLQ", triggerData)
        SERVICE_BUS_NAMESPACE = self.get_resource_name("SUMOBRTaskQNS", "Microsoft.ServiceBus/namespaces")
        SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE = f"{SERVICE_BUS_NAMESPACE}.servicebus.windows.net"
        SERVICE_BUS_QUEUE_NAME = "blobrangetaskqueue"
        servicebus_client = ServiceBusClient(SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE, self.azure_credential)

        with servicebus_client:
            sender = servicebus_client.get_queue_sender(queue_name=SERVICE_BUS_QUEUE_NAME)
            with sender:
                sender.send_messages(ServiceBusMessage(json.dumps(triggerData)))

    def change_sumo_endpoint(self):
        self.logger.info("Changing sumo logic endpoint")
        website_client = WebSiteManagementClient(credential=self.azure_credential, subscription_id=self.subscription_id)
        FunctionAppWebsiteName = self.get_resource_name("SUMOBRTaskConsumer", "Microsoft.Web/sites")
        response = website_client.web_apps.list_application_settings(resource_group_name=self.resource_group_name,name=FunctionAppWebsiteName)
        current_properties = response.properties
        current_properties["SumoLogEndpoint"] = current_properties["SumoLogEndpoint"].replace("==","")
        response = website_client.web_apps.update_application_settings(resource_group_name=self.resource_group_name,app_settings={"properties": current_properties}, name=FunctionAppWebsiteName)

    def subtest_DLQ_func_logs(self):

        self.change_sumo_endpoint()
        time.sleep(300)  # wait for settings change to reflect
        self.upload_message_in_service_bus()
        time.sleep(300)  # after 5 minutes DLQ function gets triggered
        app_insights = self.get_resource('Microsoft.Insights/components')

        azurefunction = "BlobTaskConsumer"
        captured_output = self.fetchlogs(app_insights.name, azurefunction)

        retry_error_message = "Retry error:"
        self.assertTrue(self.filter_logs(captured_output, 'message', retry_error_message),
                        f"No retry message found in {azurefunction} azure function logs")

        azurefunction = "DLQTaskConsumer"
        captured_output = self.fetchlogs(app_insights.name, azurefunction)

        successful_sent_message = "Successfully sent to Sumo, Exiting now."
        self.assertTrue(self.filter_logs(captured_output, 'message', successful_sent_message),
                        f"No success message found in {azurefunction} azure function logs")

        successful_dlq_delete_message = "Successfully deleted message from DLQ."
        self.assertTrue(self.filter_logs(captured_output, 'message', successful_dlq_delete_message),
                        f"No success delete dlq message found in {azurefunction} azure function logs")

        error_messages = self.filter_logs(captured_output, 'severityLevel', '3')
        self.assertFalse(error_messages,
                         f"Error messages found in {azurefunction} azure function logs: {error_messages}")

        warning_messages = self.filter_logs(captured_output, 'severityLevel', '2')
        self.assertFalse(warning_messages,
                         f"Warning messages found in {azurefunction} azure function logs: {warning_messages}")


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

    @classmethod
    def create_container(cls, test_container_name):
        if not cls.block_blob_service.exists(test_container_name):
            cls.block_blob_service.create_container(test_container_name)
            cls.logger.info("creating container %s" % test_container_name)

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
        with open(self.FIXTURE_FILE) as fp:
            json_data = json.load(fp)["records"]
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
            # removing spaces(added by json.loads) using separators
            file_bytes = json.dumps(data_block, separators=(',', ':'))
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
        with open(self.FIXTURE_FILE) as logfile:
            all_lines = logfile.readlines()
        return [all_lines[:2], all_lines[2:5], all_lines[5:7]] + self.get_chunks(all_lines[7:], 2)

    def get_chunks(self, l, s):
        return [l[i:i+s] for i in range(0, len(l), s)]

    def get_log_data(self):
        all_lines = []
        with open(self.FIXTURE_FILE) as logfile:
            all_lines = logfile.readlines()
        return [all_lines[:2], all_lines[2:5], all_lines[5:7], all_lines[7:]]

    def get_blob_formatted_data(self):
        all_lines = []
        with open(self.FIXTURE_FILE) as logfile:
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
