import os
import sys
import json
from azure.mgmt.eventhub import EventHubManagementClient
from azure.eventhub import EventHubProducerClient
from azure.eventhub.exceptions import EventHubError


sys.path.insert(0, '../../test_utils')
from basetest import BaseTest


class BaseEventHubTest(BaseTest):

    def _parse_template(self):
        template_path = os.path.join(os.path.abspath('..'), 'src',
                                     self.template_name)

        self.logger.info("reading template from %s" % template_path)
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
        eventhub_keys = eventhub_client.namespaces.list_keys(self.resource_group_name, namespace_name, defaultauthorule_name)

        producer = EventHubProducerClient.from_connection_string(
            conn_str=eventhub_keys.primary_connection_string,
            eventhub_name=event_hub_name
        )

        with producer:
            try:
                producer.send_batch(event_data_list)
            except ValueError:  # Size exceeds limit. This shouldn't happen if you make sure before hand.
                self.logger.info(
                    "Size of the event data list exceeds the size limit of a single send")
            except EventHubError as eh_err:
                self.logger.error("Sending error: {}".format(eh_err))

        self.logger.info("Event inserted")
