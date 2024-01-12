import sys
import unittest
from io import StringIO
from datetime import timedelta
from baseeventhubtest import BaseEventHubTest
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
class TestEventHubMetrics(BaseEventHubTest):

    def setUp(self):
        self.RESOURCE_GROUP_NAME = "azure_metric_unittest"
        self.resourcegroup_location = "eastus"
        self.template_name = "azuredeploy_metrics.json"
        self.event_hub_namespace_prefix = "SumoMetricsNamespace"
        self.eventhub_name = "insights-metrics-pt1m"
        super(TestEventHubMetrics, self).setUp()
        
    def test_pipeline(self):
        self.create_resource_group()
        self.deploy_template()
        self.assertTrue(self.resource_group_exists(self.RESOURCE_GROUP_NAME)) 
        self.insert_mock_metrics_in_EventHub('metrics_fixtures.json')
        self.check_success_log()
        self.check_error_log()
        self.check_warning_log()
    
    def fetchlogs(self):
        client = LogsQueryClient(self.azure_credential)
            
        query = '''union
                        *,
                        app('sumometricsappinsights6dbfe7sr3obyi').traces
                    | where operation_Id == "69f9a532d7cb04dd3ba6254c3d682458"'''
            
        response = client.query_workspace('800e2f15-1fa6-4c5a-a3f5-db8f647ee8a1', query, timespan=timedelta(days=1))
            
        if response.status == LogsQueryStatus.PARTIAL:
            error = response.partial_error
            data = response.partial_data
        elif response.status == LogsQueryStatus.SUCCESS:
            data = response.tables
        for table in data:
            for col in table.columns:
                print(col + "    ", end="")
            for row in table.rows:
                for item in row:
                    print(item, end="")
                print("\n")

    
    def check_success_log(self):
        # Save the original stdout for later comparison
        original_stdout = sys.stdout
        sys.stdout = StringIO()

        try:
            self.fetchlogs()
        except Exception as e:
            print("An unexpected error occurred during the test:")
            print("Exception", e)

        # Capture the output
        captured_output = sys.stdout.getvalue()
        
        # Assertions
        self.assertIn('Sent all metric data to Sumo. Exit now.', captured_output)
        
        # Reset redirect.
        sys.stdout = original_stdout

    def check_error_log(self):
        # Save the original stdout for later comparison
        original_stdout = sys.stdout
        sys.stdout = StringIO()

        try:
            self.fetchlogs()
        except Exception as e:
            print("An unexpected error occurred during the test:")
            print("Exception", e)

        # Capture the output
        captured_output = sys.stdout.getvalue()

        # Aassertions
        self.assertIn('"LogLevel":"Error"', captured_output)
        
        # Reset redirect.
        sys.stdout = original_stdout

    def check_warning_log(self):
        # Save the original stdout for later comparison
        original_stdout = sys.stdout
        sys.stdout = StringIO()

        try:
            self.fetchlogs()
        except Exception as e:
            print("An unexpected error occurred during the test:")
            print("Exception", e)

        # Capture the output
        captured_output = sys.stdout.getvalue()

        # Aassertions
        self.assertIn('"LogLevel":"Warning"', captured_output)

        # Reset redirect.
        sys.stdout = original_stdout

if __name__ == '__main__':
    unittest.main()