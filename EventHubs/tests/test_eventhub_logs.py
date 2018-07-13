import unittest
import datetime
from baseeventhubtest import BaseEventHubTest


class TestEventHubLogs(BaseEventHubTest):

    def setUp(self):
        super(TestEventHubLogs, self).setUp()
        self.RESOURCE_GROUP_NAME = "TestEventHubLogs-%s" % (
            datetime.datetime.now().strftime("%d-%m-%y-%H-%M-%S"))
        self.STORAGE_ACCOUNT_NAME = "sumoapplogs"
        self.function_name_prefix = "EventHubs_Logs"
        self.template_name = 'azuredeploy_logs.json'
        self.event_hub_namespace_prefix = "SumoAzureLogsNamespace"
        self.log_table_name = "AzureWebJobsHostLogs%d%02d" % (
            datetime.datetime.now().year, datetime.datetime.now().month)
        self.eventhub_name = 'insights-operational-logs'

    def test_pipeline(self):
        self.create_resource_group()
        self.deploy_template()
        print("Testing Stack Creation")
        self.assertTrue(self.resource_group_exists(self.RESOURCE_GROUP_NAME))
        self.table_service = self.get_table_service()
        self.insert_mock_logs_in_EventHub('activity_log_fixtures.json')
        self.check_error_logs()

    def check_error_logs(self):
        print("sleeping 1min for function execution")
        query = "PartitionKey eq 'R2'"
        self.wait_for_table_results(query)
        self.assertTrue(self.get_row_count(query) > 0)

        rows = self.table_service.query_entities(
            self.log_table_name, filter=query)

        haserr = False
        for row in rows.items:
            print("LogRow: ", row["FunctionName"], row["HasError"])
            if row["FunctionName"].startswith(self.function_name_prefix) and row["HasError"]:
                haserr = True

        self.assertTrue(not haserr)


if __name__ == '__main__':
    unittest.main()
