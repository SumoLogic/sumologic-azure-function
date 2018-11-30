Sumo Logic Azure Functions [![Build Status](https://travis-ci.org/SumoLogic/sumologic-azure-function.svg?branch=master)](https://travis-ci.org/SumoLogic/sumologic-azure-function)
==============================

# Introduction
This repository contains a collection of Azure functions to collect data and send to Sumo Logic cloud service, and a library called sumo-function-utils for these functions.

Following integrations are present. For more info look at their respective ReadMe files.

| FunctionName | Description | Collection Use Cases | Setup Documentation
| -------------| ----------- | -------------- | ------------------- |
|[Sumo Logic Azure Event Hub Integration](EventHubs)| This [solution](https://help.sumologic.com/Send-Data/Collect-from-Other-Data-Sources/Azure_Monitoring) creates a data pipeline for collecting logs/metrics from Eventhub.It includes separate ARM Templates for Metrics and Logs.| [Azure Audit App](https://help.sumologic.com/Send-Data/Applications-and-Other-Data-Sources/Azure-Audit/Azure-Audit-App-Dashboards) [Azure Active Directory App](https://help.sumologic.com/Send-Data/Applications-and-Other-Data-Sources/Azure_Active_Directory/Install_the_Azure_Active_Directory_App_and_View_the_Dashboards) [Azure SQL App](https://help.sumologic.com/Send-Data/Applications-and-Other-Data-Sources/Azure_SQL/Install_the_Azure_SQL_App_and_View_the_Dashboards)| [Docs for Logs](https://help.sumologic.com/Send-Data/Applications-and-Other-Data-Sources/Azure-Audit/02Collect-Logs-for-Azure-Audit-from-Event-Hub) <br/> [Docs for Metrics](https://help.sumologic.com/Send-Data/Applications-and-Other-Data-Sources/Azure-Audit/02Collect-Logs-for-Azure-Audit-from-Event-Hub)|
|[Sumo Logic Azure Blob Storage Integration](BlockBlobReader) | This [solution](https://help.sumologic.com/Send-Data/Collect-from-Other-Data-Sources/Azure_Blob_Storage) event-based pipeline for shipping monitoring data from Azure Blob Storage to an HTTP source on Sumo Logic.| [Azure Web Apps](https://help.sumologic.com/Send-Data/Applications-and-Other-Data-Sources/Azure-Web-Apps/Azure-Web-Apps-Dashboards) | [Docs for Logs](https://help.sumologic.com/Send-Data/Applications-and-Other-Data-Sources/Azure-Web-Apps/01Collect-Logs-for-Azure-Web-Apps) |


## For Developers
Each integration is structured in three folders
* src/     - contains actual source files
* target/  - directory used by azure's github integration to fetch source code
* tests/   - contains integration tests


### TLS 1.2 Requirement

Sumo Logic only accepts connections from clients using TLS version 1.2 or greater. To utilize the content of this repo, ensure that it's running in an execution environment that is configured to use TLS 1.2 or greater.
