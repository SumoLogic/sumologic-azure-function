import os
import random
import string
import datetime
import unittest
import json
from time import sleep
import sys
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import BlobBlock
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode
from azure.mgmt.storage import StorageManagementClient
from azure.cosmosdb.table.tableservice import TableService
from azure.mgmt.eventgrid import EventGridManagementClient
from azure.mgmt.eventhub import EventHubManagementClient
from azure.mgmt.eventgrid.models import (
    EventHubEventSubscriptionDestination,
    EventSubscriptionFilter,
    EventSubscription
)

# from azure.servicebus import ServiceBusService
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
        self.block_blob_service = self.get_teststorage_blockblob_service()
        self.create_offset_table()
        self.create_container()
        self.create_event_subscription()
        # import ipdb;ipdb.set_trace()
        self.insert_mock_logs_in_BlobStorage("log")
        self.insert_mock_logs_in_BlobStorage("csv")
        # import ipdb;ipdb.set_trace()
        sleep(120)
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

    def deploy_template(self):
        print("Deploying template")
        deployment_name = "%s-Test-%s" % (datetime.datetime.now().strftime(
            "%d-%m-%y-%H-%M-%S"), self.RESOURCE_GROUP_NAME)
        template_data = self._parse_template()
        deployment_properties = {
            'mode': DeploymentMode.incremental,
            'template': template_data
        }

        deployresp = self.resource_client.deployments.create_or_update(
            self.RESOURCE_GROUP_NAME,
            deployment_name,
            deployment_properties
        )
        deployresp.wait()
        print("ARM Template deployed", deployresp.status())

    def get_resource_name(self, resprefix, restype):
        for item in self.resource_client.resources.list_by_resource_group(self.RESOURCE_GROUP_NAME):
            if (item.name.startswith(resprefix) and item.type == restype):
                return item.name
        raise Exception("%s Resource Not Found" % (resprefix))

    def get_random_name(self, length=32):
        return ''.join(random.choice(string.ascii_lowercase) for i in range(length))

    def get_teststorage_blockblob_service(self):
        storage_client = StorageManagementClient(self.credentials,
                                                 self.subscription_id)
        storage_keys = storage_client.storage_accounts.list_keys(
            self.test_storage_res_group, self.test_storageaccount_name)
        acckey = storage_keys.keys[0].value
        block_blob_service = BlockBlobService(account_name=self.test_storageaccount_name,
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

    def create_storage_account():
        pass

    def create_container(self):
        if not self.block_blob_service.exists(self.test_container_name):
            self.block_blob_service.create_container(self.test_container_name)
            print("Creating container %s" % self.test_container_name)

    def delete_container(self):
        if self.block_blob_service.exists(self.test_container_name):
            self.block_blob_service.delete_container(self.test_container_name)

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

    def get_current_blocks(self, test_filename):
        blocks = []
        if self.block_blob_service.exists(self.test_container_name,
                                          test_filename):
            blockslist = self.block_blob_service.get_block_list(
                self.test_container_name, test_filename, None, 'all')
            for block in blockslist.committed_blocks:
                blocks.append(BlobBlock(id=block.id))
        return blocks

    def get_json_data(self):
        pass

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
        print("Inserting mock logs in BlobStorage")
        blocks = []
        datahandler = {'log': 'get_log_data',
                       'csv': 'get_csv_data',
                       'json': 'get_json_data'}
        test_filename = self.test_filename + "." + file_ext
        blocks = self.get_current_blocks(test_filename)
        for data_block in getattr(self, datahandler.get(file_ext))():
            blocks = self.create_or_update_blockblob(self.test_container_name,
                                                     test_filename,
                                                     data_block, blocks)

        print("inserted %s" % (blocks))

    def print_invocation_logs(self):
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

        self.assertTrue(not haserr)

    def _parse_template(self):
        template_path = os.path.join(os.path.abspath('..'), 'src',
                                     self.template_name)

        print("Reading template from %s" % template_path)
        with open(template_path, 'r') as template_file_fd:
            template_data = json.load(template_file_fd)

        return template_data

if __name__ == '__main__':
    unittest.main()
