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

Modify the run_integration_test.sh file with below parameters
```console

AZURE_SUBSCRIPTION_ID=`<Your azure subscription id, to obtain it refer docs https://learn.microsoft.com/en-us/azure/azure-portal/get-subscription-tenant-id#find-your-azure-subscription>`
AZURE_CLIENT_ID=`Your application id which you can get after registering application. Refer https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app#register-an-application`
AZURE_CLIENT_SECRET=`Generate client secret by referring docs https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app#add-credentials`
AZURE_TENANT_ID=`You tenant id, to obtain it refer docs https://learn.microsoft.com/en-us/azure/azure-portal/get-subscription-tenant-id#find-your-microsoft-entra-tenant`
AZURE_DEFAULT_REGION=`eastus`
SUMO_ACCESS_ID=`<Generate access id and access key https://help.sumologic.com/docs/manage/security/access-keys/#create-your-access-key>`
SUMO_ACCESS_KEY=`<Generate access id and access key https://help.sumologic.com/docs/manage/security/access-keys/#create-your-access-key>`
SUMO_DEPLOYMENT=`Enter one of the allowed values au, ca, de, eu, fed, in, jp, us1 or us2. Visit https://help.sumologic.com/APIs/General-API-Information/Sumo-Logic-Endpoints-and-Firewall-Security`
```
