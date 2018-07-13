import os
import sys
sys.path.insert(0, '../../test_utils')
from basetest import BaseTest
import json
from time import sleep
from requests import Session
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.eventhub import EventHubManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.cosmosdb.table.tableservice import TableService
from azure.servicebus import ServiceBusService


class BaseEventHubTest(BaseTest):

    def setUp(self):
        self.create_credentials()
        self.resource_client = ResourceManagementClient(self.credentials,
                                                        self.subscription_id)
        try:
            self.sumo_endpoint_url = os.environ["SumoEndpointURL"]
        except KeyError:
            raise Exception("SumoEndpointURL environment variables are not set")

        self.repo_name, self.branch_name = self.get_git_info()

    def tearDown(self):
        if self.resource_group_exists(self.RESOURCE_GROUP_NAME):
            self.delete_resource_group()

    def get_resource_name(self, resprefix, restype):
        for item in self.resource_client.resources.list_by_resource_group(self.RESOURCE_GROUP_NAME):
            if (item.name.startswith(resprefix) and item.type == restype):
                return item.name
        raise Exception("%s Resource Not Found" % (resprefix))

    def get_row_count(self, query):
        rows = self.table_service.query_entities(
            self.log_table_name, filter="PartitionKey eq 'R2'", select='PartitionKey')

        return len(rows.items)

    def wait_for_table_results(self, query):
        max_retries = 50
        while(max_retries > 0 and (not (self.table_service.exists(
              self.log_table_name) and self.get_row_count(query) > 0))):
            print("waiting for logs creation...", max_retries)
            sleep(15)
            max_retries -= 1

    def get_table_service(self):
        storage_client = StorageManagementClient(self.credentials,
                                                 self.subscription_id)
        STORAGE_ACCOUNT_NAME = self.get_resource_name(self.STORAGE_ACCOUNT_NAME, "Microsoft.Storage/storageAccounts")
        storage_keys = storage_client.storage_accounts.list_keys(
            self.RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME)
        acckey = storage_keys.keys[0].value
        table_service = TableService(account_name=STORAGE_ACCOUNT_NAME,
                                     account_key=acckey)
        return table_service

    def insert_mock_logs_in_EventHub(self, filename):
        print("Inserting fake logs in EventHub")
        namespace_name = self.get_resource_name(self.event_hub_namespace_prefix, "Microsoft.EventHub/namespaces")

        defaultauthorule_name = "RootManageSharedAccessKey"

        eventhub_client = EventHubManagementClient(self.credentials,
                                                   self.subscription_id)

        ehkeys = eventhub_client.namespaces.list_keys(
            self.RESOURCE_GROUP_NAME, namespace_name, defaultauthorule_name)

        sbs = ServiceBusService(
            namespace_name,
            shared_access_key_name=defaultauthorule_name,
            shared_access_key_value=ehkeys.primary_key,
            request_session=Session()
        )
        mock_logs = json.load(open(filename))
        print("inserting %s" % (mock_logs))
        sbs.send_event(self.eventhub_name, json.dumps(mock_logs))

        print("Event inserted")

    def _parse_template(self):
        template_path = os.path.join(os.path.abspath('..'), 'src',
                                     self.template_name)

        print("Reading template from %s" % template_path)
        with open(template_path, 'r') as template_file_fd:
            template_data = json.load(template_file_fd)

        template_data["parameters"]["SumoEndpointURL"]["defaultValue"] = self.sumo_endpoint_url
        template_data["parameters"]["sourceCodeBranch"]["defaultValue"] = self.branch_name
        template_data["parameters"]["sourceCodeRepositoryURL"]["defaultValue"] = self.repo_name

        return template_data
