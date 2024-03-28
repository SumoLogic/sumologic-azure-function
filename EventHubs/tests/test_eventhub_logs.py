import unittest
from datetime import datetime
import time
import json
from baseeventhubtest import BaseEventHubTest
from azure.eventhub import EventData


class TestEventHubLogs(BaseEventHubTest):

    def setUp(self):
        super(TestEventHubLogs, self).setUp()
        self.resource_group_name = "TestEventHubLogs-%s" % (
            datetime.now().strftime("%d-%m-%y-%H-%M-%S"))
        self.function_name_prefix = "EventHubs_Logs"
        self.template_name = 'azuredeploy_logs.json'
        self.event_hub_namespace_prefix = "SumoAzureLogsNamespace"
        self.eventhub_name = 'insights-operational-logs'
        self.successful_sent_message = 'Sent all data to Sumo. Exit now.'
        self.expected_resource_count = 7

    def test_pipeline(self):
        self.create_resource_group(
            self.resourcegroup_location, self.resource_group_name)
        self.deploy_template()
        print("Testing Stack Creation")
        self.assertTrue(self.resource_group_exists(self.resource_group_name))
        self.insert_mock_logs_in_EventHub('activity_log_fixtures.json')
        
    def insert_mock_logs_in_EventHub(self, filename):
        print("Inserting fake logs in EventHub")
        
        with open(filename, 'r') as template_file_fd:
            mock_logs = json.load(template_file_fd)
            mock_logs = json.dumps(mock_logs)
            mock_logs = mock_logs.replace("2018-03-07T14:23:51.991Z", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
            mock_logs = mock_logs.replace("C088DC46", "%d-%s" % (1, str(int(time.time()))))

        event_data_list = [EventData(mock_logs)]
        # print("inserting %s" % (mock_logs))
        self.send_event_data_list(self.event_hub_namespace_prefix, self.eventhub_name, event_data_list)


if __name__ == '__main__':
    unittest.main()
