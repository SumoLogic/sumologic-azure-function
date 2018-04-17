import unittest
import json
from time import sleep
import os
import sys
sys.path.insert(0, '../../test_utils')
from basetest import BaseTest
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode
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
        self.template_name = 'azuredeploy_metrics.json'

    def tearDown(self):
        if self.resource_group_exists(self.RESOURCE_GROUP_NAME):
            self.delete_resource_group()

    def test_pipeline(self):

        self.create_resource_group()
        self.deploy_template()
        print("Testing Stack Creation")
        self.assertTrue(self.resource_group_exists(self.RESOURCE_GROUP_NAME))
        self.insert_mock_logs_in_EventHub()
        self.check_error_logs()

    def deploy_template(self):

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
        print("Deploying Template", deployresp.status())

    def get_resource_name(self, resprefix, restype):
        for item in self.resource_client.resources.list_by_resource_group(self.RESOURCE_GROUP_NAME):
            if (item.name.startswith(resprefix) and item.type == restype):
                return item.name
        raise Exception("%s Resource Not Found" % (resprefix))

    def insert_mock_logs_in_EventHub(self):
        print("Inserting fake logs in EventHub")
        namespace_name = self.get_resource_name("SumoMetricsNamespace", "Microsoft.EventHub/namespaces")
        eventhub_name = 'insights-metrics-pt1m'
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
        sbs.send_event(eventhub_name, json.dumps(mock_logs))

        print("Event inserted")

    def check_error_logs(self):
        print("sleeping 1min for function execution")
        sleep(60)
        storage_client = StorageManagementClient(self.credentials,
                                                 self.subscription_id)
        STORAGE_ACCOUNT_NAME = self.get_resource_name("sumometlogs", "Microsoft.Storage/storageAccounts")
        storage_keys = storage_client.storage_accounts.list_keys(
            self.RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME)
        acckey = storage_keys.keys[0].value
        table_service = TableService(account_name=STORAGE_ACCOUNT_NAME,
                                     account_key=acckey)
        table = table_service.list_tables().items[0]  # flaky

        rows = table_service.query_entities(
            table.name, filter="PartitionKey eq 'R2'")

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

        return template_data

if __name__ == '__main__':
    unittest.main()
