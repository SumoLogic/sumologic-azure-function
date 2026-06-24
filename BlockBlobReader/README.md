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

Integration tests are in `BlockBlobReader/tests` folder and unit tests are in `sumo-function-utils/tests` folder.

#### Service Principal

A shared service principal is available for the team via 1Password in the **"App Content team"** vault. Use the credentials from there to configure `run_integration_test.sh`.

#### Permissions

| Role | Scope | Purpose |
|------|-------|---------|
| Contributor | Subscription | Create/deploy resource groups, function apps, storage, Event Hub, Service Bus |
| User Access Administrator | `sumo-blockblob-integration-test-do-not-delete` | Assign Storage Blob Data Reader role to function app managed identity |
| Azure Service Bus Data Sender | Subscription | Send messages to Service Bus queue during DLQ validation |

**How to set up permissions:**
1. Raise a helpdesk ticket to assign **User Access Administrator** scoped to the resource group `sumo-blockblob-integration-test-do-not-delete` (one-time request).
2. Once the SP has User Access Administrator, it can self-assign **Contributor** and **Azure Service Bus Data Sender** at subscription level:
   ```bash
   az role assignment create \
     --assignee <service-principal-app-id> \
     --role "Contributor" \
     --scope /subscriptions/<subscription-id>
   ```

#### One-time setup (admin required)

The resource group `sumo-blockblob-integration-test-do-not-delete` must exist and the SP must have `User Access Administrator` scoped to it. This resource group persists across test runs — **do not delete it**.

```bash
az group create -n sumo-blockblob-integration-test-do-not-delete -l centralus

az role assignment create \
  --assignee <service-principal-app-id> \
  --role "User Access Administrator" \
  --scope /subscriptions/<subscription-id>/resourceGroups/sumo-blockblob-integration-test-do-not-delete
```

#### Environment Variables

Modify the `run_integration_test.sh` file with below parameters:
```console
AZURE_SUBSCRIPTION_ID=`<Your azure subscription id, refer https://learn.microsoft.com/en-us/azure/azure-portal/get-subscription-tenant-id#find-your-azure-subscription>`
AZURE_CLIENT_ID=`<Your application id, refer https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app#register-an-application>`
AZURE_CLIENT_SECRET=`<Generate client secret, refer https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app#add-credentials>`
AZURE_TENANT_ID=`<Your tenant id, refer https://learn.microsoft.com/en-us/azure/azure-portal/get-subscription-tenant-id#find-your-microsoft-entra-tenant>`
AZURE_DEFAULT_REGION=`eastus`
SUMO_ACCESS_ID=`<Generate access key https://help.sumologic.com/docs/manage/security/access-keys/#create-your-access-key>`
SUMO_ACCESS_KEY=`<Generate access key https://help.sumologic.com/docs/manage/security/access-keys/#create-your-access-key>`
SUMO_DEPLOYMENT=`<One of: au, ca, de, eu, fed, in, jp, us1, us2. Refer https://help.sumologic.com/APIs/General-API-Information/Sumo-Logic-Endpoints-and-Firewall-Security>`
TEST_STORAGE_RESOURCE_GROUP=`sumo-blockblob-integration-test-do-not-delete`
```

#### Running

```bash
cd BlockBlobReader/tests
source .venv/bin/activate
source run_integration_test.sh
```

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

