# Sumo Logic Azure Event Hub Integration
This solution creates a data pipeline for shipping monitoring data out of eventhub to Sumo Logic HTTP source endpoint.

## About the Configuration Process
Sumo provides Azure Resource Management (ARM) templates to build the pipelines, one for logs, one for metrics. Each template creates an event hub to which Azure Monitor streams logs or metrics, an Azure function for sending monitoring data on to Sumo, and storage accounts to which the function writes its own log messages about successful and failed transmissions.

You download an ARM template, edit it to add the URL of your HTTP source, copy the template into Azure Portal, and deploy it. Then, you can start exporting monitoring data to EventHub.

This solution enables you to collect:

* [Activity Logs](https://help.sumologic.com/Send-Data/Applications-and-Other-Data-Sources/Azure-Audit/02Collect-Logs-for-Azure-Audit-from-Event-Hub)
* [Diagnostics Logs](https://help.sumologic.com/Send-Data/Collect-from-Other-Data-Sources/Azure_Monitoring/Collect_Logs_from_Azure_Monitor) and [Metrics](https://help.sumologic.com/Send-Data/Collect-from-Other-Data-Sources/Azure_Monitoring/Collect_Metrics_from_Azure_Monitor) which can be exported via Azure Monitor

![EventHub Collection Data Pipeline](https://s3.amazonaws.com/appdev-cloudformation-templates/AzureEventHubCollection.png)

## Building the function
Currently ARM template is integrated with github and for each functions
EventHubs/target/logs_build/EventHubs_Logs - Function for ingesting Activity Logs
EventHubs/target/metrics_build/EventHubs_Metrics - Function for ingesting Metrics Data

## For Developers
`npm run build`
This command copies required files in two directories logs_build(used for activity logs ingestions) and metrics_build(used for metrics data(in diagnostic settings) ingestion)

Integrations tests are in EventHubs/tests folder and unit tests are in sumo-function-utils/tests folder

