import unittest
import time
import json
from datetime import datetime
from baseeventhubtest import BaseEventHubTest
from azure.eventhub import EventData


class TestEventHubMetrics(BaseEventHubTest):
    
    @classmethod
    def setUpClass(self):
        datetime_value = datetime.now().strftime("%d-%m-%y-%H-%M-%S")
        self.collector_name = "azure_metric_unittest-%s" % (datetime_value)
        self.source_name = "metric_data-%s" % (datetime_value)
        self.source_category = "azure_metric_logs-%s" % (datetime_value)

        super(TestEventHubMetrics, self).setUpClass()
        
        # resource group
        self.resource_group_name = "EventHubMetrics-%s" % (datetime_value)
        self.template_name = "azuredeploy_metrics.json"
        self.event_hub_namespace_prefix = "SMNamespace"
        self.eventhub_name = "insights-metrics-pt1m"
        self.function_name = "EventHubs_Metrics"
        
    def test_01_pipeline(self):
        self.create_resource_group(
            self.resourcegroup_location, self.resource_group_name)
        
        self.deploy_template()
        self.assertTrue(self.resource_group_exists(self.resource_group_name))

    def test_02_resource_count(self):
        expected_resource_count = 7
        self.check_resource_count(expected_resource_count)

    def test_03_func_logs(self):
        successful_sent_message = 'Sent all metric data to Sumo. Exit now.'

        self.insert_mock_metrics_in_EventHub('metrics_fixtures.json')
        time.sleep(300)
        app_insights = self.get_resource('Microsoft.Insights/components')
        captured_output = self.fetchlogs(app_insights.name)
        
        self.assertTrue(self.filter_logs(captured_output, 'message', successful_sent_message),
                        "No success message found in azure function logs")
        
        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '3'),
                         "Error messages found in azure function logs")
        
        self.assertFalse(self.filter_logs(captured_output, 'severityLevel', '2'),
                         "Warning messages found in azure function logs")
    
    def insert_mock_metrics_in_EventHub(self, filename):
        self.logger.info("inserting fake metrics in EventHub")
        
        with open(filename, 'r') as template_file_fd:
            mock_logs = json.load(template_file_fd)
            mock_logs = json.dumps(mock_logs)
            mock_logs = mock_logs.replace("2018-03-07T14:23:51.991Z", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
            mock_logs = mock_logs.replace("C088DC46", "%d-%s" % (1, str(int(time.time()))))

        event_data_list = [EventData(mock_logs)]
        self.send_event_data_list(self.event_hub_namespace_prefix, self.eventhub_name, event_data_list)


if __name__ == '__main__':
    unittest.main()