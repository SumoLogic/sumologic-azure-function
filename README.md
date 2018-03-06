# Introduction
This repository contains a collection of Azure functions to collect data and send to Sumo Logic cloud service, and a library called sumo-function-utils for these functions.



## Deploying via ARM
Go to Resource Groups Service and create a Resource Group.
Click on + icon on left and search for template deployment.Click on it.Then in new window click on create.
Now choose build your own template in the editor option and in new window upload azuredeploy.json file and click on save.
In new window check the T&C and click on Purchase.

If you get any error it may be due to multiple deployments of this function so delete old deployments before creating new one.

This will most of the resources and configurations. Some specific Azure integration require extra configuration.

EventHubs:
*  Click on Storage Account Service and select sumoazureauditfaildata(for EventHubs function) or sumometfaildata(for EventHubs_metrics function) storage account.
*  Then select container under Blob and create a container by clicking on + button.
*  Input azureaudit-failover(for EventHubs function) or sumomet-failover(for EventHubs_metrics function) as name and choose private in public access level

##Building the function
Currently ARM template is integrated with github and for each functions
logs_build/EventHubs - Function for ingesting Activity Logs
metrics_build/EventHubs_Metrics - Function for ingesting Metrics Data

##For Developers
`npm run build`
This command copies required files in two directories logs_build(used for activity logs ingestions) and metrics_build(used for metrics data(in diagnostic settings) ingestion)
