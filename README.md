# Introduction
This repository contains a collection of Azure functions to collect data and send to Sumo Logic cloud service, and a library called sumo-function-utils for these functions.

## Create a Function App:
You need a Function app to host the execution of all your Sumo functions. Each Function App lets you group functions as a logic unit for easier management, deployment and sharing of resources. Please follow the section [Create a function app](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-azure-function) to create a new function app. We recommend that you go with a standard plan  [App Service](https://docs.microsoft.com/en-us/azure/app-service/azure-web-sites-web-hosting-plans-in-depth-overview) instead of the dynamic one [Consumption plan](https://docs.microsoft.com/en-us/azure/azure-functions/functions-scale) (which allows you to pay for the time the functions run, but imposes some delay). To create an App Service plan, follow the section *Create an App Service plan* in the link above.

Once you have a function app, follow the instructions under each specific Azure integration you are looking for (e.g EventHubs, etc.)

## Deploying via ARM
Go to Resource Groups Service and create a Resource Group.
Click on + icon on left and search for template deployment.Click on it.Then in new window click on create.
Now choose build your own template in the editor option and in new window upload azuredeploy.json file and click on save.
In new window check the T&C and click on Purchase.

This will most of the resources and configurations. Some specific Azure integration require extra configuration.

EventHubs:
*  Click on Storage Account Service and select sumoazureauditfaildata storage account.
*  Then select container under Blob and create a container by clicking on + button.
*  Input azureaudit-failover as name and choose private in public access level
