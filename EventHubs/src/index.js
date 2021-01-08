///////////////////////////////////////////////////////////////////////////////////
//           Function to read from an Azure EventHubs to SumoLogic               //
///////////////////////////////////////////////////////////////////////////////////

var sumoHttp = require('./sumoclient');
var dataTransformer = require('./datatransformer');
var sumoClient;

function setSourceCategory(context, msg) {
    /*
    // The below snippet gives an example on how to route logs to different source category based on log category and resource
    msg._sumo_metadata = msg._sumo_metadata || {};
    var sourcecategory;
    switch(msg.category) {
        // fall-through feature of the switch case so all the cases above Resource Health will have same auditlogs source category
        case "Administrative":
        case "Security":
        case "Service Health":
        case "Alert":
        case "Recommendation":
        case "Policy":
        case "Autoscale":
        case "Resource Health":
            sourcecategory = "<overridden sourcecategory1 ex Azure/auditlogs>"
            break;
        case "ApplicationGatewayAccessLog":
        case "ApplicationGatewayPerformanceLog":
        case "ApplicationGatewayFirewallLog":
            // You can also have switch inside switch and have source categories based on resource name
            var applicationGatewayName = msg.resourceId.split("APPLICATIONGATEWAYS/").pop();
            switch(applicationGatewayName) {
                case "<application gateway name for prod>":
                    sourcecategory = "prod/azure/network/applicationgateway";
                    break;
                case "<application gateway name for dev>":
                    sourcecategory = "dev/azure/network/applicationgateway";
                    break;
            }
            break;
        case "QueryStoreWaitStatistics":
        case "QueryStoreRuntimeStatistics"
        case "ResourceUsageStats"
        case "Errors"
            sourcecategory = "<overridden sourcecategory3 ex Azure/MYSQL>";
            break;
        default:
            sourcecategory = "<default sourcecategory>"
            break;
    }
    msg._sumo_metadata["sourceCategory"] = sourcecategory;

    */
}

module.exports = function (context, eventHubMessages) {
    //var options ={ 'urlString':process.env.APPSETTING_SumoSelfEventHubBadEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true};
    var options ={ 'urlString':process.env.APPSETTING_SumoLogsEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true};

    sumoClient = new sumoHttp.SumoClient(options,context,failureHandler,successHandler);
    var transformer = new dataTransformer.Transformer();
    var messageArray = transformer.azureAudit(eventHubMessages);
    messageArray.forEach( msg => {
        setSourceCategory(context, msg);
        sumoClient.addData(msg);
    });
    context.log(messageArray.map(function(x) { return JSON.stringify(x);}).join("\n"));

    function failureHandler(msgArray,ctx) {
        ctx.log("Failed to send to Sumo, backup to storageaccount now");
        if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
            context.bindings.outputBlob = messageArray.map(function(x) { return JSON.stringify(x);}).join("\n");
            context.done();
        }
    }
    function successHandler(ctx) {
        ctx.log('Successfully sent to Sumo');
        if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
            ctx.log('Sent all data to Sumo. Exit now.');
            context.done();
        }
    }
    context.log("Flushing the rest of the buffers:");
    sumoClient.flushAll();

};

