# Sumo Logic Azure Blob Storage Integration for AppendBlobs
This contains the function to read from an Azure Blob Storage, then forward data to a Sumo Logic.

## About the Configuration Process
Sumo provides an Azure Resource Management (ARM) template to build most of the components in the pipeline. The template creates:

An event hub to which Azure Event Grid routes create append blobs events.
A Service Bus for storing tasks.
Three Azure functions—AppendBlobFileTracker, AppendBlobTaskConsumer, and AppendBlobTaskProducer—that are responsible for sending monitoring data to Sumo.
You download the Sumo-provided ARM template, upload the template to the Azure Portal, set the parameters that identify the URL of your Sumo HTTP source and and the connection string of for the Azure Storage Account (where Azure services export their logs), and deploy the template. After deployment, you create an Event Grid subscription with a Azure Storage Account as publisher and the event hub created by the ARM template as the subscriber.


## Building the function
Currently ARM template is integrated with github and for each functions
* AppendBlobReader/target/producer_build/AppendBlobFileTracker - Function for Creating tasks(json object with start and end bytes).
* AppendBlobReader/target/consumer_build/AppendBlobTaskConsumer - Function for Downloading Append blobs and ingesting to Sumo
* AppendBlobReader/target/dlqprocessor_build/AppendBlobTaskConsumer -  Function for retrying failed tasks.

## For Developers
`npm run build`
This command copies required files in AppendBlobReader/target/ directory


