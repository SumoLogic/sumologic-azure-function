import json
import os
import sys

sys.path.insert(0, '../../test_utils')
from basetest import BaseTest


class BaseBlockBlobTest(BaseTest):
  
    def tearDown(self):
        if self.resource_group_exists(self.RESOURCE_GROUP_NAME):
            self.delete_resource_group()
        self.delete_container()
        self.delete_source(self.collector_id, self.sumo_source_id)
        self.delete_collector(self.collector_id)
        self.sumologic_cli.session.close()
    
    def _parse_template(self):
        template_path = os.path.join(os.path.abspath('..'), 'src',
                                     self.template_name)

        print("Reading template from %s" % template_path)
        with open(template_path, 'r') as template_file_fd:
            template_data = json.load(template_file_fd)

        template_data["parameters"]["SumoEndpointURL"]["defaultValue"] = self.sumo_endpoint_url
        template_data["parameters"]["sourceCodeBranch"]["defaultValue"] = self.branch_name
        template_data["parameters"]["sourceCodeRepositoryURL"]["defaultValue"] = self.repo_name
        template_data["parameters"]["StorageAccountName"]["defaultValue"] = self.test_storageaccount_name
        template_data["parameters"]["StorageAccountResourceGroupName"]["defaultValue"] = self.test_storage_res_group
        template_data["parameters"]["StorageAccountRegion"]["defaultValue"] = self.test_storageAccountRegion
        template_data["parameters"]["location"]["defaultValue"] = self.resourcegroup_location
        

        return template_data
