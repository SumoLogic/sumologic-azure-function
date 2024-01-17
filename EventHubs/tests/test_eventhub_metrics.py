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
        
    def test_pipeline(self):
        self.create_resource_group()
        self.deploy_template()
        self.assertTrue(self.resource_group_exists(self.RESOURCE_GROUP_NAME)) 
        self.insert_mock_metrics_in_EventHub('metrics_fixtures.json')
        time.sleep(300)  # Due to latency, logs are available after few mins.
        self.check_success_log()
        self.check_error_log()
        self.check_warning_log()
    
    def fetchlogs(self, query):
        original_stdout = sys.stdout
        sys.stdout = StringIO()

        try:
            client = LogsQueryClient(self.azure_credential)
            response = client.query_workspace(self.get_Workspace_Id(), query, timespan=timedelta(minutes=15))
                
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

        sys.stdout = original_stdout

        return output
    
    def check_success_log(self):
        app_insights = self.get_resource('Microsoft.Insights/components')
        query = f"union *, app('{app_insights.name}').traces | where message == 'Sent all metric data to Sumo. Exit now.' | where operation_Name == 'EventHubs_Metrics' | where customDimensions.Category == 'Function.EventHubs_Metrics.User'"
        captured_output = self.fetchlogs(query)
        haslog = False
        if ('Sent all metric data to Sumo. Exit now.' in captured_output):
            haslog = True
        self.assertTrue(haslog)

    def check_error_log(self):
        app_insights = self.get_resource('Microsoft.Insights/components')
        query = f"union *, app('{app_insights.name}').traces | where message == '""LogLevel"":""Error""' | where operation_Name == 'EventHubs_Metrics' | where customDimensions.Category == 'Function.EventHubs_Metrics.User'"
        captured_output = self.fetchlogs(query)
        haserr = False
        if ('"LogLevel":"Error"' in captured_output):
            haserr = True
        self.assertTrue(not haserr)

    def check_warning_log(self):
        app_insights = self.get_resource('Microsoft.Insights/components')
        query = f"union *, app('{app_insights.name}').traces | where message == '""LogLevel"":""Warning""' | where operation_Name == 'EventHubs_Metrics' | where customDimensions.Category == 'Function.EventHubs_Metrics.User'"
        captured_output = self.fetchlogs(query)
        haswarn = False
        if ('"LogLevel":"Warning"' in captured_output):
            haswarn = True
        self.assertTrue(not haswarn)

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