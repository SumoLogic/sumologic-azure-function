import unittest
import json
from time import sleep
import os
from basetest import BaseTest
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode
from azure.mgmt.eventhub import EventHubManagementClient
from azure.servicebus import ServiceBusService
import datetime
from requests import Session


class TestEventHubLogs(BaseTest):

    def setUp(self):
        self.create_credentials()
        self.RESOURCE_GROUP_NAME = "TestEventHubLogs-%s" % (
            datetime.datetime.now().strftime("%d-%m-%y-%H-%M-%S"))

        self.resource_client = ResourceManagementClient(self.credentials,
                                                        self.subscription_id)
        self.template_name = 'azuredeploy_activity_logs.json'
        self.directory_name = 'EventHubs'

    def tearDown(self):
        if self.resource_group_exists(self.RESOURCE_GROUP_NAME):
            self.delete_resource_group()

    def test_lambda(self):

        self.create_resource_group()
        self.deploy_template()
        print("Testing Stack Creation")
        self.assertTrue(self.resource_group_exists(self.RESOURCE_GROUP_NAME))
        self.insert_mock_logs_in_EventHub()
        # self.check_consumed_messages_count()

    def deploy_template(self):
        deployment_name = "%s-Test-%s" % (datetime.datetime.now().strftime(
            "%d-%m-%y-%H-%M-%S"), self.directory_name)
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

    def get_eventhub_namespace(self):
        for item in self.resource_client.resources.list_by_resource_group(self.RESOURCE_GROUP_NAME):
            if (item.name.startswith("SumoAzureAudit") and item.type == "Microsoft.EventHub/namespaces"):
                return item.name
        return "SumoAzureAudit"

    def insert_mock_logs_in_EventHub(self):
        print("Inserting fake logs in EventHub")
        namespace_name = self.get_eventhub_namespace()
        eventhub_name = 'insights-operational-logs'
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
        mock_logs = json.load(open('activity_log_fixtures.json'))
        print("inserting %s" % (mock_logs))
        sbs.send_event(eventhub_name, json.dumps(mock_logs))

        print("Event inserted")

    def check_consumed_messages_count(self):
        sleep(120)
        # currently checking manually todo: find way of reading invocation logs

    def _parse_template(self):
        template_path = os.path.join(os.path.abspath('..'),
                                     self.directory_name, 'Node.js',
                                     self.template_name)

        print("Reading template from %s" % template_path)
        with open(template_path, 'r') as template_file_fd:
            template_data = json.load(template_file_fd)

        return template_data

if __name__ == '__main__':
    unittest.main()
