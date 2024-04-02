import unittest
import os
import json
import time
import subprocess
import logging
import sys
from datetime import datetime, timedelta
from sumologic import SumoLogic
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.loganalytics import LogAnalyticsManagementClient
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.mgmt.resource.resources.models import Deployment, DeploymentMode


class BaseTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        BaseTest.allTestsPassed = True
        cls.logger = logging.getLogger(__name__)
        cls.logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
        LOG_FORMAT = "%(levelname)s | %(asctime)s | %(threadName)s | %(filename)s | %(message)s"
        logFormatter = logging.Formatter(LOG_FORMAT)
        consoleHandler = logging.StreamHandler(sys.stdout)
        consoleHandler.setFormatter(logFormatter)
        cls.logger.addHandler(consoleHandler)

        # acquire a credential object.
        cls.azure_credential = DefaultAzureCredential()

        # retrieve subscription ID from environment variable.
        cls.subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
        cls.resourcegroup_location = os.environ["AZURE_DEFAULT_REGION"]

        # obtain the management object for resources.
        cls.resource_client = ResourceManagementClient(
            cls.azure_credential, cls.subscription_id)
        cls.repo_name, cls.branch_name = cls.get_git_info()

        # sumologic: collector and source
        cls.sumologic_cli = SumoLogic(
            os.environ["SUMO_ACCESS_ID"], os.environ["SUMO_ACCESS_KEY"], cls.api_endpoint(os.environ["SUMO_DEPLOYMENT"]))
        cls.collector_id = cls.create_collector(cls.collector_name)
        cls.sumo_source_id, cls.sumo_endpoint_url = cls.create_source(
            cls.collector_id, cls.source_name)

    @classmethod
    def tearDownClass(cls):
        if BaseTest.allTestsPassed:
            if cls.resource_group_exists(cls.resource_group_name):
                cls.delete_resource_group(cls.resource_group_name)

            cls.delete_source(cls.collector_id, cls.sumo_source_id)
            cls.delete_collector(cls.collector_id)
        else:
            cls.logger.info("Skipping resource group and sumo resource deletion")
        cls.sumologic_cli.session.close()

    def run(self, *args, **kwargs):
        testResult = super(BaseTest, self).run(*args, **kwargs)
        BaseTest.allTestsPassed = BaseTest.allTestsPassed and testResult.wasSuccessful()
        return testResult

    @classmethod
    def resource_group_exists(cls, group_name):
        # grp: name,id,properties

        for grp in cls.resource_client.resource_groups.list():
            if grp.name == group_name:
                if grp.properties.provisioning_state == "Succeeded":
                    return True

        return False

    @classmethod
    def create_resource_group(cls, location, resource_group_name):
        resp = cls.resource_client.resource_groups.create_or_update(
            resource_group_name, {'location': location})
        cls.logger.info('created ResourceGroup: {}, state: {}'.format(
            resp.name, resp.properties.provisioning_state))

    def get_resource(self, restype):
        for item in self.resource_client.resources.list_by_resource_group(self.resource_group_name):
            if (item.type == restype):
                return item
        raise Exception("%s Resource Not Found" % (restype))

    def get_resource_name(self, resprefix, restype):
        for item in self.resource_client.resources.list_by_resource_group(self.resource_group_name):
            if (item.name.startswith(resprefix) and item.type == restype):
                return item.name
        raise Exception("%s Resource Not Found" % (resprefix))

    def get_resources(self, resource_group_name):
        return self.resource_client.resources.list_by_resource_group(resource_group_name)

    def get_Workspace_Id(self):
        workspace = self.get_resource(
            'microsoft.operationalinsights/workspaces')
        client = LogAnalyticsManagementClient(
            credential=self.azure_credential,
            subscription_id=self.subscription_id,
        )

        response = client.workspaces.get(
            resource_group_name=self.resource_group_name,
            workspace_name=workspace.name,
        )
        return response.customer_id

    @classmethod
    def delete_resource_group(cls, resource_group_name):
        resp = cls.resource_client.resource_groups.begin_delete(
            resource_group_name)
        resp.wait()
        cls.logger.info('deleted ResourceGroup:{}, status: {}'.format(
            resource_group_name, resp.status()))

    def deploy_template(self):
        self.logger.info("deploying template")
        deployment_name = "%s-Test-%s" % (datetime.now().strftime("%d-%m-%y-%H-%M-%S"), self.resource_group_name)
        template_data = self._parse_template()

        deployment_properties = {
            'mode': DeploymentMode.INCREMENTAL,
            'template': template_data
        }

        deployment = Deployment(properties=deployment_properties)

        deployment_operation_poller = self.resource_client.deployments.begin_create_or_update(
            self.resource_group_name,
            deployment_name,
            deployment
        )

        deployment_result = deployment_operation_poller.result()
        if not deployment_operation_poller.done():
            self.logger.warning("Deployment process incomplete")

        self.logger.info(
            f"ARM Template deployment completed with result: {deployment_result}")

    @classmethod
    def get_git_info(cls):
        repo_slug = "SumoLogic/sumologic-azure-function"
        try:
            branch_name = subprocess.check_output("git branch --show-current", stderr=subprocess.STDOUT, shell=True)
            branch_name = branch_name.decode("utf-8").strip()

        except Exception:
            branch_name = os.environ["SOURCE_BRANCH"]

        if not branch_name or branch_name == "undefined":
            raise Exception("Error getting branch name")

        repo_name = f"https://github.com/{repo_slug}"

        cls.logger.info(
            f"Testing for repo {repo_name} in branch {branch_name}")

        return repo_name, branch_name

    @classmethod
    def api_endpoint(cls, sumo_deployment):
        if sumo_deployment == "us1":
            return "https://api.sumologic.com/api"
        elif sumo_deployment in ["ca", "au", "de", "eu", "jp", "us2", "fed", "in"]:
            return "https://api.%s.sumologic.com/api" % sumo_deployment
        else:
            return 'https://%s-api.sumologic.net/api' % sumo_deployment

    @classmethod
    def create_collector(cls, collector_name):
        cls.logger.info("creating collector")
        collector_id = None
        collector = {
            'collector': {
                'collectorType': 'Hosted',
                'name': collector_name,
                'description': "",
                'category': None
            }
        }
        try:
            resp = cls.sumologic_cli.create_collector(collector, headers=None)
            collector_id = json.loads(resp.text)['collector']['id']
            cls.logger.info("created collector {}".format(collector_name))
        except Exception as e:
            raise Exception(e)

        return collector_id

    @classmethod
    def delete_collector(cls, collector_id):
        sources = cls.sumologic_cli.sources(collector_id, limit=10)
        if len(sources) == 0:
            cls.sumologic_cli.delete_collector(
                {"collector": {"id": collector_id}})
            cls.logger.info("deleted collector")

    @classmethod
    def create_source(cls, collector_id, source_name):
        cls.logger.info("creating source")
        endpoint = source_id = None
        params = {
            "sourceType": "HTTP",
            "name": source_name,
            "messagePerRequest": False,
            "multilineProcessingEnabled": True,
            "category": cls.source_category
        }

        try:
            resp = cls.sumologic_cli.create_source(collector_id, {"source": params})
            data = resp.json()['source']
            source_id = data["id"]
            endpoint = data["url"]
            cls.logger.info("created source {}".format(source_name))
        except Exception as e:
            raise Exception(e)
        return source_id, endpoint

    @classmethod
    def delete_source(cls, collector_id, source_id):
        cls.sumologic_cli.delete_source(
            collector_id, {"source": {"id": source_id}})
        cls.logger.info("deleted source")

    def fetchlogs(self, app_insights, function_name):
        result = []
        try:
            client = LogsQueryClient(self.azure_credential)
            query = f"app('{app_insights}').traces | where operation_Name == '{function_name}' | project operation_Id, timestamp, message, severityLevel"
            response = client.query_workspace(
                self.get_Workspace_Id(), query, timespan=timedelta(hours=1))

            if response.status == LogsQueryStatus.FAILURE:
                raise Exception(f"LogsQueryError: {response.message}")
            elif response.status == LogsQueryStatus.PARTIAL:
                data = response.partial_data
                error = response.partial_error
                self.logger.error("partial_error: {}".format(error))
            elif response.status == LogsQueryStatus.SUCCESS:
                data = response.tables

            for table in data:
                for row in table.rows:
                    row_dict = {str(col): str(item)
                                for col, item in zip(table.columns, row)}
                    result.append(row_dict)
        except Exception as e:
            self.logger.error("Exception in fetch logs: {}".format(e))

        return result

    def filter_logs(self, logs, key, value):
        return [d.get(key) for d in logs if value in d.get(key, '')]

    def filter_log_Count(self, logs, key, value):
        return sum(1 for log in logs if value in log[key])

    def check_resource_count(self, expected_resource_count):
        resouces = list(filter(lambda x: not x.name.startswith("Failure Anomalies"), list(self.get_resources(self.resource_group_name))))
        resource_count = len(resouces)
        self.assertTrue(resource_count == expected_resource_count,
                        f"resource count: {resource_count}  of resource group {self.resource_group_name} differs from expected count : {expected_resource_count}")

    @classmethod
    def fetch_sumo_query_results(cls, query='_sourceCategory="azure_br_logs" | count', relative_time_in_minutes=15):

        toTime = datetime.utcnow()
        fromTime = toTime + timedelta(minutes=-1*relative_time_in_minutes)

        cls.logger.info(
            f"query: {query}, fromTime: {fromTime.isoformat(timespec='seconds')}, toTime: {toTime.isoformat(timespec='seconds')}")

        delay = 5
        LIMIT = 10000

        search_job = cls.sumologic_cli.search_job(
            query, fromTime.isoformat(timespec="seconds"), toTime.isoformat(
                timespec="seconds"), timeZone='UTC', byReceiptTime=True, autoParsingMode='Manual')


        status = cls.sumologic_cli.search_job_status(search_job)
        while status['state'] != 'DONE GATHERING RESULTS':
            if status['state'] == 'CANCELLED':
                break
            time.sleep(delay)
            status = cls.sumologic_cli.search_job_status(search_job)

        count = 0
        if status['state'] == 'DONE GATHERING RESULTS':
            count = status['recordCount']
            limit = count if count < LIMIT and count != 0 else LIMIT  # compensate bad limit check
            result = cls.sumologic_cli.search_job_records(search_job, limit=limit)
            cls.logger.info(f"source result: {result}")
            return result
        return

    @classmethod
    def fetch_sumo_MetrixQuery_results(cls, query='_sourceCategory="azure_br_logs" | count', relative_time_in_minutes=15):

        toTime = datetime.now()
        fromTime = toTime + timedelta(minutes=-1*relative_time_in_minutes)

        cls.logger.info(
            f"query: {query}, fromTime: {fromTime.isoformat(timespec='seconds')}, toTime: {toTime.isoformat(timespec='seconds')}")

        search_result = cls.sumologic_cli.search_metrics(query, int(fromTime.timestamp())*1000, int(toTime.timestamp())*1000)
        cls.logger.info(f"source result: {search_result}")
        return search_result
