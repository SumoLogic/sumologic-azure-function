import sys
import unittest
import time
from io import StringIO
from datetime import timedelta, datetime
from baseeventhubtest import BaseEventHubTest
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.mgmt.loganalytics import LogAnalyticsManagementClient

class TestEventHubMetrics(BaseEventHubTest):

    def setUp(self):
        super(TestEventHubMetrics, self).setUp()
        self.RESOURCE_GROUP_NAME = "EventHubMetrics-%s" % (datetime.now().strftime("%d-%m-%y-%H-%M-%S"))
        self.template_name = "azuredeploy_metrics.json"
        self.event_hub_namespace_prefix = "SMNamespace"
        self.eventhub_name = "insights-metrics-pt1m"
        self.function_name = "EventHubs_Metrics"
        self.resource_count = 8
        
    def test_pipeline(self):
        self.create_resource_group()
        self.deploy_template()
        self.assertTrue(self.resource_group_exists(self.RESOURCE_GROUP_NAME)) 
        self.insert_mock_metrics_in_EventHub('metrics_fixtures.json')
        time.sleep(300)  # Due to latency, logs are available after few mins.
        self.get_resource_count()
        app_insights = self.get_resource('Microsoft.Insights/components')
        captured_output = self.fetchlogs(app_insights.name)
        self.check_success_log(captured_output)
        self.check_error_log(captured_output)
        self.check_warning_log(captured_output)
    
    def fetchlogs(self, app_insights):
        result = []
        try:
            client = LogsQueryClient(self.azure_credential)
            query = f"app('{app_insights}').traces | where operation_Name == '{self.function_name}' | project operation_Id, timestamp, message, severityLevel"
            response = client.query_workspace(self.get_Workspace_Id(), query, timespan=timedelta(hours=1))
                
            if response.status == LogsQueryStatus.PARTIAL:
                data = response.partial_data
            elif response.status == LogsQueryStatus.SUCCESS:
                data = response.tables

            for table in data:
                for row in table.rows:
                    row_dict = {str(col): str(item) for col, item in zip(table.columns, row)}
                    result.append(row_dict)
        except Exception as e:
            print("An unexpected error occurred during the test:")
            print("Exception", e)

        return result
    
    def get_resource_count(self):
        return len(list(self.resource_client.resources.list_by_resource_group(self.RESOURCE_GROUP_NAME)))
    
    def filter_logs(self, logs, key, value):
        return value in [d.get(key) for d in logs]
    
    def check_resource_count(self):
        self.assertTrue(self.get_resource_count() == self.resource_count)

    def check_success_log(self, logs):
        successful_sent_message = 'Sent all metric data to Sumo. Exit now.'
        self.assertTrue(self.filter_logs(logs, 'message', successful_sent_message))

    def check_error_log(self, logs):
        self.assertTrue(not self.filter_logs(logs, 'severityLevel', '3'))

    def check_warning_log(self, logs):
        self.assertTrue(not self.filter_logs(logs, 'severityLevel', '2'))

    def get_Workspace_Id(self):
        workspace = self.get_resource('microsoft.operationalinsights/workspaces')
        client = LogAnalyticsManagementClient(
            credential=self.azure_credential,
            subscription_id=self.subscription_id,
        )

        response = client.workspaces.get(
            resource_group_name=self.RESOURCE_GROUP_NAME,
            workspace_name=workspace.name,
        )
        return response.customer_id
    

if __name__ == '__main__':
    unittest.main()