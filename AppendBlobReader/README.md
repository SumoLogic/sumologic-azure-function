# Sumo Logic Azure Blob Storage Integration for AppendBlobs
This contains the function to read append blob files from an Azure Storage Account and ingest to SumoLogic.

## About the Configuration Process
Sumo provides an Azure Resource Management (ARM) template to build most of the components in the pipeline. The template creates:

* An event hub to which Azure Event Grid routes create append blobs events.
* A Service Bus for storing tasks.
* Three Azure functions—AppendBlobFileTracker, AppendBlobTaskProducer, and AppendBlobTaskConsumer—that are responsible for sending monitoring data to Sumo.
* A storage account to which the Azure functions write their log messages about successful and failed transmissions.

For more details checkout the [documentation](https://help.sumologic.com/Send-Data/Collect-from-Other-Data-Sources/Azure_Blob_Storage/Collect_Logs_from_Azure_AppendBlob_Storage)

![Append Blob Storage Data Collection Pipeline](https://s3.amazonaws.com/appdev-cloudformation-templates/AppendBlobReader.png)

## For Developers

### Code structure
Currently ARM template is integrated with github and for each functions build folder is present in `AppendBlobReader/target` directory
* AppendBlobReader/target/producer_build/AppendBlobFileTracker - Function for Creating file metadata in file `FileOffsetMap` table in storage account.
* AppendBlobReader/target/consumer_build/AppendBlobTaskConsumer - Function for Downloading Append blobs and ingesting to SumoLogic
* AppendBlobReader/target/appendblob_producer_build/AppendBlobTaskProducer -  Function for periodically polling `FileOffsetMap` table and creating tasks in Service Bus to be consumed by consumer function

### Updating target directory
Make all the code changes in `AppendBlobReader/src` directory, once all the changes are completed, run below command to update target directory.
`npm run build`
This command copies required files in `AppendBlobReader/target` directory

### Run Unit Test
Integrations tests are in `AppendBlobReader/tests` folder and unit tests are in sumo-`function-utils/tests` folder
```console

export AZURE_SUBSCRIPTION_ID=`<Your azure subscription id, to obtain it refer docs https://learn.microsoft.com/en-us/azure/azure-portal/get-subscription-tenant-id#find-your-azure-subscription>`
export AZURE_CLIENT_ID=`Your application id which you can get after registering application. Refer https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app#register-an-application`
export AZURE_CLIENT_SECRET=`Generate client secret by referring docs https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app#add-credentials`
export AZURE_TENANT_ID=`You tenant id, to obtain it refer docs https://learn.microsoft.com/en-us/azure/azure-portal/get-subscription-tenant-id#find-your-microsoft-entra-tenant`
export AZURE_DEFAULT_REGION=`eastus`
export SUMO_ACCESS_ID=`<Generate access id and access key https://help.sumologic.com/docs/manage/security/access-keys/#create-your-access-key>`
export SUMO_ACCESS_KEY=`<Generate access id and access key https://help.sumologic.com/docs/manage/security/access-keys/#create-your-access-key>`
export SUMO_DEPLOYMENT=`Enter one of the allowed values au, ca, de, eu, fed, in, jp, us1 or us2. Visit https://help.sumologic.com/APIs/General-API-Information/Sumo-Logic-Endpoints-and-Firewall-Security`

```

Execute below command under `AppendBlobReader/tests` directory
`python test_appendblobreader.py`