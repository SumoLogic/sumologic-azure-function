import os
import sys
import json
from datetime import timedelta
from sumologic import SumoLogic
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.eventhub import EventHubManagementClient
from azure.eventhub import EventHubProducerClient
from azure.eventhub.exceptions import EventHubError
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.mgmt.loganalytics import LogAnalyticsManagementClient

sys.path.insert(0, '../../test_utils')
from basetest import BaseTest


class BaseEventHubTest(BaseTest):

    def setUp(self):
        self.create_credentials()
        self.resource_client = ResourceManagementClient(self.azure_credential, 
                                                        self.subscription_id)
        self.repo_name, self.branch_name = self.get_git_info()
        self.sumologic_cli = SumoLogic(os.environ["SUMO_ACCESS_ID"], os.environ["SUMO_ACCESS_KEY"], self.api_endpoint(os.environ["SUMO_DEPLOYMENT"]))
        self.collector_id = self.create_collector(self.collector_name)
        self.sumo_source_id, self.sumo_endpoint_url = self.create_source(self.collector_id, self.source_name)
        
    def tearDown(self):
        if self.resource_group_exists(self.RESOURCE_GROUP_NAME):
            self.delete_resource_group()
        self.delete_source(self.collector_id, self.sumo_source_id)
        self.delete_collector(self.collector_id)
        self.sumologic_cli.session.close()

    def get_resource_name(self, resprefix, restype):
        for item in self.resource_client.resources.list_by_resource_group(self.RESOURCE_GROUP_NAME):
            if (item.name.startswith(resprefix) and item.type == restype):
                return item.name
        raise Exception("%s Resource Not Found" % (resprefix))
    
    def get_resource(self, restype):
        for item in self.resource_client.resources.list_by_resource_group(self.RESOURCE_GROUP_NAME):
            if (item.type == restype):
                return item
        raise Exception("%s Resource Not Found" % (restype))

    def _parse_template(self):
        template_path = os.path.join(os.path.abspath('..'), 'src',
                                     self.template_name)

        print("Reading template from %s" % template_path)
        with open(template_path, 'r') as template_file_fd:
            template_data = json.load(template_file_fd)

        template_data["parameters"]["sumoEndpointURL"]["defaultValue"] = self.sumo_endpoint_url
        template_data["parameters"]["sourceCodeBranch"]["defaultValue"] = self.branch_name
        template_data["parameters"]["sourceCodeRepositoryURL"]["defaultValue"] = self.repo_name
        template_data["parameters"]["location"]["defaultValue"] = self.resourcegroup_location

        return template_data
    
    def send_event_data_list(self, event_hub_namespace_prefix, event_hub_name, event_data_list):
        
        defaultauthorule_name = "RootManageSharedAccessKey"
        namespace_name = self.get_resource_name(event_hub_namespace_prefix, "Microsoft.EventHub/namespaces")
        eventhub_client = EventHubManagementClient(self.azure_credential, self.subscription_id)
        eventhub_keys = eventhub_client.namespaces.list_keys(self.RESOURCE_GROUP_NAME, namespace_name, defaultauthorule_name)
       
        producer = EventHubProducerClient.from_connection_string(
            conn_str=eventhub_keys.primary_connection_string,
            eventhub_name=event_hub_name
        )
        
        with producer:
            try:
                producer.send_batch(event_data_list)
            except ValueError:  # Size exceeds limit. This shouldn't happen if you make sure before hand.
                print("Size of the event data list exceeds the size limit of a single send")
            except EventHubError as eh_err:
                print("Sending error: ", eh_err)

        print("Event inserted")
    
    def fetchlogs(self, app_insights):
        result = []
        try:
            client = LogsQueryClient(self.azure_credential)
            query = f"app('{app_insights}').traces | where operation_Name == '{self.function_name}' | project operation_Id, timestamp, message, severityLevel"
            response = client.query_workspace(self.get_Workspace_Id(), query, timespan=timedelta(hours=1))
                
            if response.status == LogsQueryStatus.FAILURE:
                raise Exception(f"LogsQueryError: {response.message}")
            elif response.status == LogsQueryStatus.PARTIAL:
                data = response.partial_data
                error = response.partial_error
                print("partial_error: ", error)
            elif response.status == LogsQueryStatus.SUCCESS:
                data = response.tables

            for table in data:
                for row in table.rows:
                    row_dict = {str(col): str(item) for col, item in zip(table.columns, row)}
                    result.append(row_dict)
        except Exception as e:
            print("Exception", e)

        return result
    
    def get_resources(self, resource_group_name):
        return self.resource_client.resources.list_by_resource_group(resource_group_name)
    
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
    
    def filter_logs(self, logs, key, value):
        return value in [d.get(key) for d in logs]
    
    def check_resource_count(self):
        resource_count = len(list(self.get_resources(self.RESOURCE_GROUP_NAME)))
        self.assertTrue(resource_count == self.expected_resource_count, f"resource count of resource group {self.RESOURCE_GROUP_NAME} differs from expected count : {resource_count}")

    def check_success_log(self, logs):
        self.assertTrue(self.filter_logs(logs, 'message', self.successful_sent_message), "No success message found in azure function logs")
    
    def check_error_log(self, logs):
        self.assertFalse(self.filter_logs(logs, 'severityLevel', '3'), "Error messages found in azure function logs")

    def check_warning_log(self, logs):
        self.assertFalse(self.filter_logs(logs, 'severityLevel', '2'), "Warning messages found in azure function logs")

