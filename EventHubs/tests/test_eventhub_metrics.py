import sys
import unittest
from io import StringIO
from datetime import timedelta, datetime
from baseeventhubtest import BaseEventHubTest
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
class TestEventHubMetrics(BaseEventHubTest):

    def setUp(self):
        super(TestEventHubMetrics, self).setUp()
        self.RESOURCE_GROUP_NAME = "EventHubMetrics-%s" % (str(int(datetime.now().timestamp())))
        self.template_name = "azuredeploy_metrics.json"
        self.event_hub_namespace_prefix = "SMNamespace"
        self.eventhub_name = "insights-metrics-pt1m"
        
    def test_pipeline(self):
        self.create_resource_group()
        self.deploy_template()
        self.assertTrue(self.resource_group_exists(self.RESOURCE_GROUP_NAME)) 
        self.insert_mock_metrics_in_EventHub('metrics_fixtures.json')
        self.check_success_log()
        self.check_error_log()
        self.check_warning_log()
    
    def fetchlogs(self, query):
        # Save the original stdout for later comparison
        original_stdout = sys.stdout
        sys.stdout = StringIO()

        try:
            client = LogsQueryClient(self.azure_credential)
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
        except Exception as e:
            print("An unexpected error occurred during the test:")
            print("Exception", e)

        output = sys.stdout.getvalue()

        # Reset redirect.
        sys.stdout = original_stdout

        return output
    
    def check_success_log(self):
       
        query = '''union
                    *,
                    app('sumometricsappinsights6dbfe7sr3obyi').traces
                | where operation_Id == "69f9a532d7cb04dd3ba6254c3d682458"'''
    
        captured_output = self.fetchlogs(query)

        # Assertions
        self.assertIn('Sent all metric data to Sumo. Exit now.', captured_output)

    def check_error_log(self):
        query = '''union
                    *,
                    app('sumometricsappinsights6dbfe7sr3obyi').traces
                | where operation_Id == "69f9a532d7cb04dd3ba6254c3d682458"'''
    
        captured_output = self.fetchlogs(query)

        # Aassertions
        self.assertIn('"LogLevel":"Error"', captured_output)

    def check_warning_log(self):
        query = '''union
                    *,
                    app('sumometricsappinsights6dbfe7sr3obyi').traces
                | where operation_Id == "69f9a532d7cb04dd3ba6254c3d682458"'''
    
        captured_output = self.fetchlogs(query)

        # Aassertions
        self.assertIn('"LogLevel":"Warning"', captured_output)

if __name__ == '__main__':
    unittest.main()