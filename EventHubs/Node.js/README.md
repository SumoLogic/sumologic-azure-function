# Introduction
This contains the function to read from an Azure EventHub, then forward data to a Sumo Logic [Http Source](https://help.sumologic.com/Send-Data/Sources/02Sources-for-Hosted-Collectors/HTTP-Source).
# Setup 
## Create a Function App:
Please follow the instruction in the main repo README to create a Function App.

## Prepare the Environment Variables  
We will define the required information for the function under the hosting Function App's [settings](https://docs.microsoft.com/en-us/azure/azure-functions/functions-how-to-use-azure-function-app-settings). From the Function App, go to *Settings > Application settings > Manage application settings* then define the environment variables there (make sure to Save when defining a new variable). We will need the followings: 
- A variable (e.g. named SumoEndpoint) containing the Sumo HTTP endpoint url.
- A variable (e.g. named AzureEventHubConnectionString) containing a connection string for the source EventHub. To get the connection string, from the Azure portal, click *More services > Event Hubs* then select the EventHub *namespace* containing the source Event Hub. Continue to select *SETTINGS > Shared access policies* then either create a new or select an existing access policy with Send and Listen permission. Use any connection string under that policy.
- A variable (e.g. named StorageConnectionString) containing a connection string for a storage account. We'll use this storage account to store any data that failed to be sent to Sumo on rare occasions.To get a connection string, from the Azure portal, click *More services > Storage accounts* then select the storage account, and continue to *SETTINGS > Access keys* and select any connection string.
NOTE: You also need to create a blob container under that storage account, for example: *azureaudit-failover*.


## Deploy the function:
Once all the above environment variables are defined, create your function as follows: 
1. Select a Function App to host the funtion. From there, select *Functions > +*, select JavaScript as the language, and EventHubTrigger. Provide a name for your function, for the Event Hub name, select the source Event Hub name (for example, *insights-operational-logs*); for the Event Hub connection, select the variable defining the connection string for the event hub in the step above from the dropdown list. Click Create to finish.
2. Once the function is created, click on its name, then *View files > Upload*. Upload all files under the [sumo-function-utils](https://github.com/SumoLogic/sumologic-azure-function/tree/sumo-function-utils/sumo-function-utils/lib) there.
3. Go back to the default content under index.js, then replace it with our index.js content. Make sure the urlString parameter value inside the function matches the name of the Sumo Endpoint environment variable (KEEP prefix *process.env.APPSETTINGS_*). 
4. Change the function integration: select Integrate under the function, go to Advanced Editor and add an storage output binding to the *bindings* array:
<code>
```
{
      "type": "blob",
      "name": "outputBlob",
      "path": "azureaudit-failover/{rand-guid}",
      "connection": "<USE THE NAME OF ENV VARIABLE FOR THE STORAGE ACCOUNT>",
      "direction": "out"
}
```
</code>

Note: here "azureaudit-failover" is the name of the container to host the failover data created under the previous section. If you use a different name.

Your final bindings array should look something like this: 
<code>
```
{
  "bindings": [
    {
      "type": "eventHubTrigger",
      "name": "eventHubMessages",
      "direction": "in",
      "path": "insights-operational-logs",
      "connection": "AzureLabsEventHub_DevSharedAccess_EVENTHUB",
      "cardinality": "many",
      "consumerGroup": "$Default"
    },
    {
      "type": "blob",
      "name": "outputBlob",
      "path": "azureaudit-failover/{rand-guid}",
      "connection": "sumologicstorage_STORAGE",
      "direction": "out"
    }
  ],
  "disabled": false
}
```
</code>
5. Finally, test the function by going to index.js and click Run. On the Sumo side, use [Live Tail](https://help.sumologic.com/Search/Live-Tail) on the target endpoint to see the data.
