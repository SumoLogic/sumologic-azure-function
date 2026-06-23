# Sumo Logic Azure Blob Storage Integration for AppendBlobs
This contains the function to read append blob files from an Azure Storage Account and ingest to SumoLogic.

## About the Configuration Process
Sumo provides an Azure Resource Management (ARM) template to build most of the components in the pipeline. The template creates:

* An event hub to which Azure Event Grid routes create append blobs events.
* A Service Bus for storing tasks.
* Three Azure functions — `AppendBlobFileTracker`, `AppendBlobTaskProducer`, and `AppendBlobTaskConsumer` that are responsible for sending monitoring data to Sumo.
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

### Run Integration Tests

Integration tests are in `AppendBlobReader/tests` folder and unit tests are in `sumo-function-utils/tests` folder.

#### Service Principal

A shared service principal is available for the team via 1Password in the **"App Content team"** vault. Use the credentials from there to configure `run_integration_test.sh`.

#### Permissions

| Role | Scope | Purpose |
|------|-------|---------|
| Contributor | Subscription | Create/deploy resource groups, function apps, storage, Event Hub, Service Bus |
| User Access Administrator | `sumo-appendblob-integration-test-do-not-delete` | Assign Storage Blob Data Reader role to function app managed identity |

**How to set up permissions:**
1. Raise a helpdesk ticket to assign **User Access Administrator** scoped to the resource group `sumo-appendblob-integration-test-do-not-delete` (one-time request).
2. Once the SP has User Access Administrator, it can self-assign **Contributor** at subscription level:
   ```bash
   az role assignment create \
     --assignee <service-principal-app-id> \
     --role "Contributor" \
     --scope /subscriptions/<subscription-id>
   ```

#### One-time setup (admin required)

The resource group `sumo-appendblob-integration-test-do-not-delete` must exist and the SP must have `User Access Administrator` scoped to it. This resource group persists across test runs — **do not delete it**.

```bash
az group create -n sumo-appendblob-integration-test-do-not-delete -l centralus

az role assignment create \
  --assignee <service-principal-app-id> \
  --role "User Access Administrator" \
  --scope /subscriptions/<subscription-id>/resourceGroups/sumo-appendblob-integration-test-do-not-delete
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
TEST_STORAGE_RESOURCE_GROUP=`sumo-appendblob-integration-test-do-not-delete`
```

#### Running

```bash
cd AppendBlobReader/tests
source .venv/bin/activate
source run_integration_test.sh
```

### Run Unit Tests

To run unit tests, first install test dependencies and then run the tests using below commands

 `npm install`

 `npm test`

## Security Fixes

  package-lock.json can be created using below command

     npm install --package-lock

  Fix the security dependencies by running below command

     npm audit fix


