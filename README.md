Sumo Logic Azure Functions ![Build Status](https://github.com/SumoLogic/sumologic-azure-function/actions/workflows/arm-template-test.yml/badge.svg)
==============================

# Introduction
This repository contains a collection of Azure functions to collect data and send to Sumo Logic cloud service, and a library called sumo-function-utils for these functions.

Following integrations are present. For more info look at their respective ReadMe files.

| FunctionName | Description | Collection Use Cases | Setup Documentation
| -------------| ----------- | -------------- | ------------------- |
|[Sumo Logic Azure Event Hub Integration for Metrics](EventHubs)| This solution creates a data pipeline for collecting metrics from Eventhub.|  [Azure SQL App](https://help.sumologic.com/docs/integrations/microsoft-azure/sql/#collect-metrics-from-azure-monitor-by-streaming-to-eventhub)| [Collect Metrics from Azure Monitor](https://help.sumologic.com/docs/send-data/collect-from-other-data-sources/azure-monitoring/collect-metrics-azure-monitor/) |
|[Sumo Logic Azure Event Hub Integration for Logs](EventHubs)| This solution creates a data pipeline for collecting logs from Eventhub.| This solution is deprecated, use [Azure Event Hubs Source for Logs](https://help.sumologic.com/docs/send-data/collect-from-other-data-sources/azure-monitoring/ms-azure-event-hubs-source/)  | 
|[Sumo Logic Azure Blob Storage Integration (block blobs)](BlockBlobReader) | This [solution](https://help.sumologic.com/docs/send-data/collect-from-other-data-sources/azure-blob-storage/block-blob/) event-based pipeline for shipping monitoring data from Azure Blob Storage (stored as block blobs) to an HTTP source on Sumo Logic.| This is used for apps which do not support exporting to Eventhub and are stored as block blob type. [Azure Network Watcher](https://help.sumologic.com/docs/integrations/microsoft-azure/network-watcher/#collecting-logs-for-the-azure-network-watcher-app) | [Collect Logs from Azure Blob Storage (Block Blobs)](https://help.sumologic.com/docs/send-data/collect-from-other-data-sources/azure-blob-storage/block-blob/collect-logs/) |
|[Sumo Logic Azure Blob Storage Integration (append blobs)](AppendBlobReader) | This [solution](https://help.sumologic.com/docs/send-data/collect-from-other-data-sources/azure-blob-storage/append-blob/) event-based pipeline for shipping monitoring data from Azure Blob Storage (stored as append blobs) to an HTTP source on Sumo Logic.| This is used for apps which do not support exporting to Eventhub and are stored as append blob type, for example [custom application logs](https://github.com/m-moris/log4j2-azure-blob-appender) | [Collect Logs from Azure Blob Storage (Append Blobs)](https://help.sumologic.com/docs/send-data/collect-from-other-data-sources/azure-blob-storage/append-blob/collect-logs/) |


## For Developers
Each integration is structured in three folders
* src/     - contains actual source files
* target/  - directory used by azure's github integration to fetch source code
* tests/   - contains integration tests

### Important Points
* WEBSITE_CONTENTAZUREFILECONNECTIONSTRING is required for Consumption and Elastic Premium plan apps running on both Windows and Linux. Although we have included it dedicated plan as well in case users want to switch the plan. [docs](https://learn.microsoft.com/en-us/azure/azure-functions/functions-app-settings#website_contentazurefileconnectionstring)
* All the functions use Windows based plans because Linux based plans do not support source control. [docs](https://learn.microsoft.com/en-us/azure/azure-functions/functions-deployment-technologies?tabs=windows#deployment-technology-availability)
* When using WEBSITE_RUN_FROM_PACKAGE = <URL>, Function apps running on Windows experience a slight increase in cold start time and you must also manually sync triggers after you publish an updated package. [docs](https://learn.microsoft.com/en-us/azure/azure-functions/run-functions-from-deployment-package#using-website_run_from_package--url)
* When running your functions from a zip package file in Azure, only zip files are currently supported and files become read-only in the Azure portal, so update & rebuild them (using create_zip.sh) before deploying. [docs](https://learn.microsoft.com/en-us/azure/azure-functions/run-functions-from-deployment-package#general-considerations)
* Currently the zip files contains all the packages, and Kudu assumes by default that deployments from zip files are ready to run and do not require additional build steps during deployment, such as `npm install`. This can be overridden by setting the SCM_DO_BUILD_DURING_DEPLOYMENT deployment setting to true. [docs](https://github.com/projectkudu/kudu/wiki/Deploying-from-a-zip-file-or-url)
* You can scale dedicated plans and apps by changing the capacity and numberOfWorkers setting. [docs](https://learn.microsoft.com/en-us/azure/app-service/manage-scale-per-app#per-app-scaling-using-azure-resource-manager)
* Function apps running on Version 1.x of the Azure Functions runtime will reach the end of life (EOL) for extended support on September 14, 2026.

## Release

### Releasing appdev package
  The new zip package gets released automatically after the tags are pushed using Github actions(Refer tagged-release in https://github.com/marvinpinto/action-automatic-releases).

  Run below commands to create and push tags
  
     git tag -a v<major.minor.patch> <commit_id>
 
     git push origin v<major.minor.patch>

### TLS 1.2 Requirement

Sumo Logic only accepts connections from clients using TLS version 1.2 or greater. To utilize the content of this repo, ensure that it's running in an execution environment that is configured to use TLS 1.2 or greater.
