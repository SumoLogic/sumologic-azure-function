# Sumo Logic Azure Blob Storage Integration
This contains the function to read from an Azure Blob Storage, then forward data to a Sumo Logic.

## About the Configuration Process
Sumo provides an Azure Resource Management (ARM) template to build most of the components in the pipeline. The template creates:

An event hub to which Azure Event Grid routes create block blobs events.
A Service Bus for storing tasks.
Three Azure functions—TaskProducer, TaskConsumer, and DLQTaskConsumer—that are responsible for sending monitoring data to Sumo.
A storage account to which the Azure functions write their log messages about successful and failed transmissions.
You download the Sumo-provided ARM template, upload the template to the Azure Portal, set the parameters that identify the URL of your Sumo HTTP source and and the connection string of for the Azure Storage Account (where Azure services export their logs), and deploythe template. After deployment, you create an Event Grid subscription with a Azure Storage Account as publisher and the event hub created by the ARM template as the subscriber.

For more details checkout the [documentation.](https://help.sumologic.com/Send-Data/Collect-from-Other-Data-Sources/Azure_Blob_Storage/Collect_Logs_from_Azure_Blob_Storage)


## Building the function
Currently ARM template is integrated with github and for each functions
* BlockBlobReader/target/producer_build/BlobTaskProducer - Function for Creating tasks(json object with start and end bytes).
* BlockBlobReader/target/consumer_build/BlobTaskConsumer - Function for Downloading block blobs and ingesting to Sumo
* BlockBlobReader/target/dlqprocessor_build/BlobTaskConsumer -  Function for retrying failed tasks.

## For Developers
`npm run build`
This command copies required files in BlockBlobReader/target/ directory

Integrations tests are in BlockBlobReader/tests folder and unit tests are in sumo-function-utils/tests folder
