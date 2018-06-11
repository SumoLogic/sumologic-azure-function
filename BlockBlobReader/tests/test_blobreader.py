import os
import datetime
import unittest
import json
import uuid
from time import sleep
import sys
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import BlobBlock
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.cosmosdb.table.tableservice import TableService
from azure.mgmt.eventgrid import EventGridManagementClient
from azure.mgmt.eventhub import EventHubManagementClient
from azure.mgmt.eventgrid.models import (
    EventHubEventSubscriptionDestination,
    EventSubscriptionFilter,
    EventSubscription
)


sys.path.insert(0, '../../test_utils')
from basetest import BaseTest


class TestBlobReaderFlow(BaseTest):

    def setUp(self):
        self.create_credentials()

        self.RESOURCE_GROUP_NAME = "TestBlobReaderLogs-%s" % (
            datetime.datetime.now().strftime("%d-%m-%y-%H-%M-%S"))

        self.resource_client = ResourceManagementClient(self.credentials,
                                                        self.subscription_id)
        self.template_name = 'blobreaderdeploy.json'
        self.log_table_name = "AzureWebJobsHostLogs%d%02d" % (
            datetime.datetime.now().year, datetime.datetime.now().month)
        self.offsetmap_table_name = "FileOffsetMap"

        self.test_storage_res_group = "ag-sumo"
        self.test_storageaccount_name = "allbloblogs"
        self.test_container_name = "testcontainer-%s" % (
            datetime.datetime.now().strftime("%d-%m-%y-%H-%M-%S"))
        self.test_filename = "testblob"
        try:
            self.sumo_endpoint_url = os.environ["SumoEndpointURL"]
            self.storage_connection_string = os.environ["StorageAcccountConnectionString"]
        except KeyError:
            raise Exception("SumoEndpointURL/StorageAcccountConnectionString environment variables are not set")

        self.repo_name, self.branch_name = self.get_git_info()

    def tearDown(self):
        if self.resource_group_exists(self.RESOURCE_GROUP_NAME):
            self.delete_resource_group()
            self.delete_event_subscription()
        self.delete_container()

    def test_pipeline(self):

        self.create_resource_group()
        self.deploy_template()
        print("Testing Stack Creation")
        self.assertTrue(self.resource_group_exists(self.RESOURCE_GROUP_NAME))
        self.table_service = self.get_table_service()
        self.block_blob_service = self.get_blockblob_service(
            self.test_storage_res_group, self.test_storageaccount_name)

        self.create_offset_table()

        self.create_container()
        self.create_event_subscription()
        sleep(5)
        log_type = os.environ.get("LOG_TYPE", "log")
        print("Inserting mock %s data in BlobStorage" % log_type)
        if log_type in ("csv", "log"):
            self.insert_mock_logs_in_BlobStorage(log_type)
        else:
            self.insert_mock_json_in_BlobStorage()
        sleep(15)
        self.print_invocation_logs()
        self.check_error_logs()

    def check_sorted_task_range():
        pass

    def check_offset_in_range():
        pass

    def extract_tasks_from_logs():
        pass

    def check_one_to_one_task_mapping():
        pass

    def get_resource_name(self, resprefix, restype):
        for item in self.resource_client.resources.list_by_resource_group(self.RESOURCE_GROUP_NAME):
            if (item.name.startswith(resprefix) and item.type == restype):
                return item.name
        raise Exception("%s Resource Not Found" % (resprefix))

    def get_random_name(self, length=32):
        return str(uuid.uuid4())

    def get_blockblob_service(self, resource_group, account_name):
        storage_client = StorageManagementClient(self.credentials,
                                                 self.subscription_id)
        storage_keys = storage_client.storage_accounts.list_keys(
            resource_group, account_name)
        acckey = storage_keys.keys[0].value
        block_blob_service = BlockBlobService(account_name=account_name,
                                              account_key=acckey)
        return block_blob_service

    def get_table_service(self):
        storage_client = StorageManagementClient(self.credentials,
                                                 self.subscription_id)
        STORAGE_ACCOUNT_NAME = self.get_resource_name("sumobrlogs", "Microsoft.Storage/storageAccounts")
        storage_keys = storage_client.storage_accounts.list_keys(
            self.RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME)
        acckey = storage_keys.keys[0].value
        table_service = TableService(account_name=STORAGE_ACCOUNT_NAME,
                                     account_key=acckey)
        # table = table_service.list_tables().items[0]  # flaky
        return table_service

    def get_event_hub_resource_id(self):
        namespace_name = self.get_resource_name("SUMOBREventHubNamespace", "Microsoft.EventHub/namespaces")
        eventhub_client = EventHubManagementClient(self.credentials,
                                                   self.subscription_id)
        ehitr = eventhub_client.event_hubs.list_by_namespace(
            self.RESOURCE_GROUP_NAME, namespace_name)
        return next(ehitr).id  # assming single eventhub

    def create_offset_table(self):
        print("creating FileOffsetMap table")
        self.table_service.create_table(self.offsetmap_table_name)

    def create_test_storage_account():
        # Todo: remove storage account allbloblogs dependency
        pass

    def create_container(self):
        if not self.block_blob_service.exists(self.test_container_name):
            self.block_blob_service.create_container(self.test_container_name)
            print("Creating container %s" % self.test_container_name)

    def delete_container(self):
        if self.block_blob_service.exists(self.test_container_name):
            self.block_blob_service.delete_container(self.test_container_name)
            print("Deleting container %s" % self.test_container_name)

    def create_event_subscription(self):
        print("creating event subscription")
        event_client = EventGridManagementClient(self.credentials, self.subscription_id)

        scope = '/subscriptions/'+self.subscription_id+'/resourceGroups/'+self.test_storage_res_group+'/providers/microsoft.storage/storageaccounts/%s' % self.test_storageaccount_name
        destination = EventHubEventSubscriptionDestination(resource_id=self.get_event_hub_resource_id())
        esfilter = EventSubscriptionFilter(**{
            "subject_begins_with": "/blobServices/default/containers/%s/" % self.test_container_name,
            "subject_ends_with": "",
            "is_subject_case_sensitive": False,
            "included_event_types": ["Microsoft.Storage.BlobCreated"]
        })
        event_subscription_info = EventSubscription(destination=destination, filter=esfilter)
        create_resp = event_client.event_subscriptions.create_or_update(scope, "testeventsubscription", event_subscription_info)
        create_resp.wait()

    def delete_event_subscription(self):
        print("deleting event subscription")
        event_client = EventGridManagementClient(self.credentials, self.subscription_id)
        scope = '/subscriptions/'+self.subscription_id+'/resourceGroups/'+self.test_storage_res_group+'/providers/microsoft.storage/storageaccounts/%s' % self.test_storageaccount_name
        event_client.event_subscriptions.delete(scope, "testeventsubscription")

    def create_or_update_blockblob(self, container_name, file_name, datalist, blocks):
        block_id = self.get_random_name()
        file_bytes = ''.join(datalist).encode()
        self.block_blob_service.put_block(container_name, file_name, file_bytes, block_id)
        blocks.append(BlobBlock(id=block_id))
        self.block_blob_service.put_block_list(container_name, file_name, blocks)
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
            self.block_blob_service.put_block(container_name, file_name, file_bytes, block_id)
            blocks.append(BlobBlock(id=block_id))
            self.block_blob_service.put_block_list(container_name, file_name, blocks)
        return blocks

    def insert_mock_json_in_BlobStorage(self):
        test_filename = self.test_filename + ".json"
        blocks = self.insert_empty_json(self.test_container_name, test_filename)
        for i, data_block in enumerate(self.get_json_data()):
            block_id = self.get_random_name()
            file_bytes = json.dumps(data_block)
            file_bytes = (file_bytes[1:-1] if i == 0 else "," + file_bytes[1:-1]).encode()
            self.block_blob_service.put_block(self.test_container_name, test_filename, file_bytes, block_id)
            blocks.insert(len(blocks)-1, BlobBlock(id=block_id))
            self.block_blob_service.put_block_list(self.test_container_name, test_filename, blocks)
        print("inserted %s" % (blocks))

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

    def insert_mock_logs_in_BlobStorage(self, file_ext):
        blocks = []
        datahandler = {'log': 'get_log_data', 'csv': 'get_csv_data'}
        test_filename = self.test_filename + "." + file_ext
        blocks = self.get_current_blocks(self.test_container_name, test_filename)
        for data_block in getattr(self, datahandler.get(file_ext))():
            blocks = self.create_or_update_blockblob(self.test_container_name,
                                                     test_filename,
                                                     data_block, blocks)

        print("inserted %s" % (blocks))

    def is_task_consumer_invoked(self):
        rows = self.table_service.query_entities(
            self.log_table_name, filter="PartitionKey eq 'I'",
            select='FunctionName')
        is_task_consumer_func_invoked = False
        for row in rows.items:
            if row.get("FunctionName") == "BlobTaskConsumer":
                is_task_consumer_func_invoked = True
                break
        return is_task_consumer_func_invoked

    def print_invocation_logs(self):

        while(not self.is_task_consumer_invoked()):
            sleep(15)

        rows = self.table_service.query_entities(
            self.log_table_name, filter="PartitionKey eq 'I'")

        for row in sorted(rows.items, key=lambda k: k['FunctionName']):
            print(row.get("FunctionName"), str(row.get('StartTime')), str(row.get('EndTime')))
            print(row.get("ErrorDetails"))
            print(row.get('LogOutput'))

    def check_error_logs(self):

        rows = self.table_service.query_entities(
            self.log_table_name, filter="PartitionKey eq 'R2'")

        haserr = False
        for row in rows.items:
            print("LogRow: ", row["FunctionName"], row["HasError"])
            if row["FunctionName"].startswith(("TaskProducer", "TaskConsumer", "DLQProcessor")) and row["HasError"]:
                haserr = True

        self.assertFalse(haserr)

    def _parse_template(self):
        template_path = os.path.join(os.path.abspath('..'), 'src',
                                     self.template_name)

        print("Reading template from %s" % template_path)
        with open(template_path, 'r') as template_file_fd:
            template_data = json.load(template_file_fd)

        template_data["parameters"]["StorageAcccountConnectionString"]["defaultValue"] = self.storage_connection_string
        template_data["parameters"]["SumoEndpointURL"]["defaultValue"] = self.sumo_endpoint_url
        template_data["parameters"]["sourceCodeBranch"]["defaultValue"] = self.branch_name
        template_data["parameters"]["sourceCodeRepositoryURL"]["defaultValue"] = self.repo_name

        return template_data

if __name__ == '__main__':
    unittest.main()
