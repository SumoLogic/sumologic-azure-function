import unittest
import json
from time import sleep
import os
import sys
sys.path.insert(0, '../../test_utils')
from basetest import BaseTest
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.eventhub import EventHubManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.cosmosdb.table.tableservice import TableService
from azure.servicebus import ServiceBusService
import datetime
from requests import Session


class TestEventHubMetrics(BaseTest):

    def setUp(self):

        self.create_credentials()
        self.RESOURCE_GROUP_NAME = "TestEventHubMetrics-%s" % (
            datetime.datetime.now().strftime("%d-%m-%y-%H-%M-%S"))
        self.function_name_prefix = "EventHubs_Metrics"
        self.resource_client = ResourceManagementClient(self.credentials,
                                                        self.subscription_id)
        self.STORAGE_ACCOUNT_NAME = "sumometlogs"
        self.template_name = 'azuredeploy_metrics.json'
        self.log_table_name = "AzureWebJobsHostLogs%d%02d" % (
            datetime.datetime.now().year, datetime.datetime.now().month)
        self.event_hub_namespace_prefix = "SumoMetricsNamespace"
        self.eventhub_name = 'insights-metrics-pt1m'
        try:
            self.sumo_endpoint_url = os.environ["SumoEndpointURL"]
        except KeyError:
            raise Exception("SumoEndpointURL environment variables are not set")

        self.repo_name, self.branch_name = self.get_git_info()

    def tearDown(self):
        if self.resource_group_exists(self.RESOURCE_GROUP_NAME):
            self.delete_resource_group()

    def test_pipeline(self):

        self.create_resource_group()
        self.deploy_template()
        print("Testing Stack Creation")
        self.assertTrue(self.resource_group_exists(self.RESOURCE_GROUP_NAME))
        self.table_service = self.get_table_service()
        self.insert_mock_logs_in_EventHub()
        self.check_error_logs()

    def get_resource_name(self, resprefix, restype):
        for item in self.resource_client.resources.list_by_resource_group(self.RESOURCE_GROUP_NAME):
            if (item.name.startswith(resprefix) and item.type == restype):
                return item.name
        raise Exception("%s Resource Not Found" % (resprefix))

    def wait_for_table_creation(self):
        max_retries = 50
        while(max_retries > 0 and (not self.table_service.exists(self.log_table_name))):
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

    def insert_mock_logs_in_EventHub(self):
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
        mock_logs = json.load(open('metrics_fixtures.json'))
        print("inserting %s" % (mock_logs))
        sbs.send_event(self.eventhub_name, json.dumps(mock_logs))

        print("Event inserted")

    def check_error_logs(self):
        print("sleeping 1min for function execution")
        self.wait_for_table_creation()
        sleep(10)  # wait for log creation

        rows = self.table_service.query_entities(
            self.log_table_name, filter="PartitionKey eq 'R2'")

        haserr = False
        for row in rows.items:
            print("LogRow: ", row["FunctionName"], row["HasError"])
            if row["FunctionName"].startswith(self.function_name_prefix) and row["HasError"]:
                haserr = True

        self.assertTrue(not haserr)

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

if __name__ == '__main__':
    unittest.main()
