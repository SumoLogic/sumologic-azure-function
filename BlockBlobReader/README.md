# Sumo Logic Azure Blob Storage Integration
This contains the function to read from an Azure Blob Storage, then forward data to a Sumo Logic.

## About the Configuration Process
Sumo provides an Azure Resource Management (ARM) template to build most of the components in the pipeline. The template creates:

* An event hub to which Azure Event Grid routes create block blobs events.
* A Service Bus for storing tasks.
* Three Azure functions — TaskProducer, TaskConsumer, and DLQTaskConsumer that are responsible for sending monitoring data to Sumo.
* A storage account to which the Azure functions write their log messages about successful and failed transmissions.

For more details checkout the [documentation](https://help.sumologic.com/Send-Data/Collect-from-Other-Data-Sources/Azure_Blob_Storage/Collect_Logs_from_Azure_Blob_Storage)

![Block Blob Storage Data Collection Pipeline](https://s3.amazonaws.com/appdev-cloudformation-templates/AzureBlobStorageCollection.png)

## For Developers

### Code structure

Currently ARM template is integrated with github and for each functions build folder is present in `BlockBlobReader/target` directory

* BlockBlobReader/target/producer_build/BlobTaskProducer - Function for Creating tasks(json object with start and end bytes).
* BlockBlobReader/target/consumer_build/BlobTaskConsumer - Function for Downloading block blobs and ingesting to Sumo
* BlockBlobReader/target/dlqprocessor_build/BlobTaskConsumer -  Function for retrying failed tasks.

### Updating target directory

Make all the code changes in `BlockBlobReader/src` directory, once all the changes are completed, run below command to update target directory.

`npm run build`

This command copies required files in `BlockBlobReader/target` directory

Integrations tests are in `BlockBlobReader/tests` folder and unit tests are in `sumo-function-utils/tests` folder

### Run Integration Tests

Integrations tests are in `BlockBlobReader/tests` folder and unit tests are in sumo-`function-utils/tests` folder

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


Execute below command under `BlockBlobReader/tests` directory

`python test_blobreader.py`

## Security Fixes

  package-lock.json can be created using below command

     npm install --package-lock

  Fix the security dependencies by running below command

     npm audit fix

## Publishing the zip

1. export the AWS_PROFILE
1. Update the tag in src/create_zip.sh file
1. Run the script

   `sh create_zip.sh` 

