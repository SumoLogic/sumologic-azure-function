# Introduction
This contains the function to read from an Azure EventHub, then forward data to a Sumo Logic [Http Source](https://help.sumologic.com/Send-Data/Sources/02Sources-for-Hosted-Collectors/HTTP-Source).
# Setup 
## Create a Function App:
Please follow the instruction in the main repo README to create a Function App.

## Prepare the Environment Variables  
We will define the required information for the function under the hosting Function App's [settings](https://docs.microsoft.com/en-us/azure/azure-functions/functions-how-to-use-azure-function-app-settings). Go to *Settings > Application settings > Manage application settings* then define the environment variables there. We need the followings: 
- A variable (eg. named SumoEndpoint) containing the Sumo HTTP endpoint url.
- A variable (eg. named AzureEventHubConnectionString) containing a connection string for the source EventHub. To get the connection string, from the Azure portal, click *More services > Event Hubs* then select the EventHub namespace containing the source Event Hub. Continue to select *SETTINGS > Shared access policies* then either create a new or select an existing access policy with Send and Listen permission. Use any connection string under that policy.
- A variable (e.
From the Azure portal, go to the Function Apps you will host the function, 

