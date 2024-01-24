Sumo Logic Azure Functions [![Build Status](https://travis-ci.org/SumoLogic/sumologic-azure-function.svg?branch=master)](https://travis-ci.org/SumoLogic/sumologic-azure-function)
==============================

# Introduction
This repository contains a collection of Azure functions to collect data and send to Sumo Logic cloud service, and a library called sumo-function-utils for these functions.

Following integrations are present. For more info look at their respective ReadMe files.

| FunctionName | Description | Collection Use Cases | Setup Documentation
| -------------| ----------- | -------------- | ------------------- |
|[Sumo Logic Azure Event Hub Integration for Metrics](EventHubs)| This solution creates a data pipeline for collecting metrics from Eventhub.|  [Azure SQL App](https://help.sumologic.com/docs/integrations/microsoft-azure/sql/#collect-metrics-from-azure-monitor-by-streaming-to-eventhub)| [Collect Metrics from Azure Monitor](https://help.sumologic.com/docs/send-data/collect-from-other-data-sources/azure-monitoring/collect-metrics-azure-monitor/) 
|[Sumo Logic Azure Event Hub Integration for Logs](EventHubs)| This solution creates a data pipeline for collecting logs from Eventhub.| This is no longer recommended mechanism, use [Azure Event Hubs Source](https://help.sumologic.com/docs/send-data/hosted-collectors/cloud-to-cloud-integration-framework/azure-event-hubs-source/)  | [Collect Logs from Azure Monitor](https://help.sumologic.com/docs/send-data/collect-from-other-data-sources/azure-monitoring/collect-logs-azure-monitor/) 
|[Sumo Logic Azure Blob Storage Integration](BlockBlobReader) | This [solution](https://help.sumologic.com/docs/send-data/collect-from-other-data-sources/azure-blob-storage/) event-based pipeline for shipping monitoring data from Azure Blob Storage to an HTTP source on Sumo Logic.| This is used for apps which do not support exporting to Eventhub. [Azure Network Watcher](https://help.sumologic.com/docs/integrations/microsoft-azure/network-watcher/#collecting-logs-for-the-azure-network-watcher-app) | [Collect Logs from Azure Blob Storage (Block Blobs)](https://help.sumologic.com/docs/send-data/collect-from-other-data-sources/azure-blob-storage/collect-logs-azure-blob-storage/) |


## For Developers
Each integration is structured in three folders
* src/     - contains actual source files
* target/  - directory used by azure's github integration to fetch source code
* tests/   - contains integration tests

## Release

### Releasing appdev package
  The new zip package gets released automatically after the tags are pushed using Github actions(Refer tagged-release in https://github.com/marvinpinto/action-automatic-releases).

  Run below commands to create and push tags
  
     git tag -a v<major.minor.patch> <commit_id>
 
     git push origin v<major.minor.patch>


### TLS 1.2 Requirement

Sumo Logic only accepts connections from clients using TLS version 1.2 or greater. To utilize the content of this repo, ensure that it's running in an execution environment that is configured to use TLS 1.2 or greater.
