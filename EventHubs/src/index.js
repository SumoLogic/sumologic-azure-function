///////////////////////////////////////////////////////////////////////////////////
//           Function to read from an Azure EventHubs to SumoLogic               //
///////////////////////////////////////////////////////////////////////////////////

var sumoHttp = require('./sumoclient');
var dataTransformer = require('./datatransformer');
var sumoClient;

function setSourceCategory(context, msg) {
    // msg._sumo_metadata = {
    //     "category": "new_source_category"
    // }
}

module.exports = function (context, eventHubMessages) {
    //var options ={ 'urlString':process.env.APPSETTING_SumoSelfEventHubBadEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true};
    var options ={ 'urlString':process.env.APPSETTING_SumoLogsEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true};


    var transformer = new dataTransformer.Transformer(context);
    var messageArray = transformer.azureAudit(eventHubMessages);
    if (messageArray.length !== 0) {
        context.log("Sending: " + messageArray.length);
        sumoClient = new sumoHttp.SumoClient(options,context,failureHandler,successHandler);
        messageArray.forEach( msg => {
            setSourceCategory(context, msg);
            sumoClient.addData(msg);
        });



        function failureHandler(msgArray,ctx) {
            ctx.log("Failed to send to Sumo, backup to storageaccount now" + ' messagesAttempted: ' + this.messagesAttempted  + ' messagesReceived: ' + this.
messagesReceived);
            if (this.messagesAttempted === this.messagesReceived) {
                context.bindings.outputBlob = messageArray.map(function(x) { return JSON.stringify(x);}).join("\n");
                context.done();
            }
        }
        function successHandler(ctx) {
             ctx.log('Successfully sent chunk to Sumo' + ' messagesAttempted: ' + this.messagesAttempted  + ' messagesReceived: ' + this.messagesReceived);
            if (this.messagesAttempted === this.messagesReceived) {
                ctx.log('Sent all data to Sumo. Exit now.');
                context.done();
            }
        }
        context.log("Flushing the rest of the buffers:");
        sumoClient.flushAll();
    } else {
        context.log("No messages to send");
        context.log(eventHubMessages);
        context.done();
    }

};

