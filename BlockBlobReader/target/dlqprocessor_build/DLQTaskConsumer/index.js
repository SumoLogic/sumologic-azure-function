///////////////////////////////////////////////////////////////////////////////////
//           Function to read from an Azure EventHubs to SumoLogic               //
///////////////////////////////////////////////////////////////////////////////////

var sumoHttp = require('./sumoclient');
var dataTransformer = require('./datatransformer');
var storage = require('azure-storage');
var servicebus = require('azure-sb');

function CSVToArray( strData, strDelimiter ){
    strDelimiter = (strDelimiter || ",");
    var objPattern = new RegExp(
        (
            // Delimiters.
            "(\\" + strDelimiter + "|\\r?\\n|\\r|^)" +

            // Quoted fields.
            "(?:\"([^\"]*(?:\"\"[^\"]*)*)\"|" +

            // Standard fields.
            "([^\"\\" + strDelimiter + "\\r\\n]*))"
        ),
        "gi"
        );
    var arrData = [[]];
    var arrMatches = null;
    while (arrMatches = objPattern.exec( strData )){
        var strMatchedDelimiter = arrMatches[ 1 ];
        if (strMatchedDelimiter.length && strMatchedDelimiter !== strDelimiter ){
            arrData.push( [] );
        }
        var strMatchedValue;
        if (arrMatches[ 2 ]){
            strMatchedValue = arrMatches[ 2 ].replace( //unescape any double quotes.
                new RegExp( "\"\"", "g" ),
                "\""
                );
        } else {
            strMatchedValue = arrMatches[ 3 ]; // We found a non-quoted value.
        }
        arrData[ arrData.length - 1 ].push( strMatchedValue );
    }
    return( arrData );
}

function hasAllHeaders(text) {
    var delimitters = new RegExp("(\\r?\\n|\\r)");
    var strMatchedDelimiter = text.match(delimitters);
    if (strMatchedDelimiter) {
        return text.split(strMatchedDelimiter[0])[0];
    } else {
        return null;
    }
}

function getHeaderRecursively(headertext, task, connectionString, context) {

    return new Promise(function(resolve, reject) {
        getData(task, connectionString, context).then(function(text) {
            headertext += text;
            var onlyheadertext = hasAllHeaders(headertext);
            if (onlyheadertext) {
                var csvHeaders = CSVToArray(onlyheadertext, ",");
                if (csvHeaders && csvHeaders[0].length > 0)
                    resolve(csvHeaders[0]);
                else {
                    reject("Error in CSVToArray parsing: " + csvHeaders)
                }
            } else {
                task.startByte = task.endByte + 1;
                task.endByte = task.startByte + bytesOffset -1;
                getHeaderRecursively(headertext, task, connectionString, context).then(function(headers) {
                    resolve(headers);
                }).catch(function(err) {
                    reject(err);
                });
                // resolve(getHeader(headertext, task, connectionString, context));
            }
        }).catch(function(err) {
            reject(err);
        });
    });

}

function getcsvHeader(containerName, blobName, context, connectionString) {
    // Todo optimize to avoid multiple request
    var blobService = storage.createBlobService(connectionString);
    var bytesOffset = 1024;
    var task = {
        containerName: containerName,
        blobName: blobName,
        startByte: 0,
        endByte: bytesOffset - 1
    };

    return getHeaderRecursively("", task, connectionString, context);
}

function csvHandler(msgtext, headers) {
    var messages = CSVToArray(msgtext, ",");
    var messageArray = [];
    if (headers.length > 0 && messages.length > 0 && messages[0].length > 0 && headers[0] === messages[0][0]) {
        messages = messages.slice(1); //removing header row
    }
    messages.forEach(function(row) {
        if (row.length == headers.length) {
            var msgobj = {};
            for (var i = headers.length - 1; i >= 0; i--) {
                msgobj[headers[i]] = row[i];
            }
            messageArray.push(msgobj);
        }
    });
    return messageArray;
}

function jsonHandler(msg) {

}


function logHandler(msg) {
    return [msg];
}

function getData(task, connectionString, context) {
    // Todo support for chunk reading(if range is large)
    // valid offset status code 206 (Partial Content).
    // invalid offset status code 416 (Requested Range Not Satisfiable)

    var blobService = storage.createBlobService(connectionString);
    var containerName = task.containerName;
    var blobName = task.blobName;
    var options = {rangeStart: task.startByte, rangeEnd: task.endByte};

    return new Promise(function(resolve, reject) {
        blobService.getBlobToText(containerName, blobName, options, function (err, blobContent, blob) {
            if (err) {
                reject(err);
            } else {
                resolve(blobContent);
            }
        });
    });

}

function messageHandler(serviceBusTask, context, connectionString, sumoClient) {
    file_ext = serviceBusTask.blobName.split(".").pop();
    var msghandler = {"log": logHandler, "csv": csvHandler, "json": jsonHandler};
    if (!(file_ext in msghandler)) {
        context.done("Unknown file extension: " + file_ext + " for blob: " + serviceBusTask.blobName);
    }

    getData(serviceBusTask, connectionString, context).then(function(msg) {
        context.log("Sucessfully downloaded blob %s %d %d", serviceBusTask.blobName, serviceBusTask.startByte, serviceBusTask.endByte);
        var messageArray;
        if (file_ext == "csv") {
            getcsvHeader(serviceBusTask.containerName, serviceBusTask.blobName, context, connectionString).then(function (headers) {
                    context.log("Received headers %s", headers.join(","));
                    messageArray =  msghandler[file_ext](msg, headers);
                    context.log("Transformed data %s", JSON.stringify(messageArray));
                    messageArray.forEach( function(msg) {
                        sumoClient.addData(msg);
                    });
                    sumoClient.flushAll();
            }).catch(function (err) {
                context.log("Error in creating json from csv " + err);
                context.done(err);
            })
        } else if (file_ext == "json") {

        } else {
            messageArray = msghandler[file_ext](msg)
            messageArray.forEach( function(msg) {
                sumoClient.addData(msg);
            });
            sumoClient.flushAll();
        }


    }).catch(function(err) {
        context.log("Failed to downloaded blob %s %d %d", serviceBusTask.blobName, serviceBusTask.startByte, serviceBusTask.endByte);
        context.done(err);
    });
}

function servicebushandler(context, serviceBusTask) {

    var options = {
        urlString: process.env.APPSETTING_SumoLogEndpoint,
        metadata: {},
        MaxAttempts: 3,
        RetryInterval: 3000,
        compress_data: true,
        clientHeader: "blobreader-azure-function"
    };
    function failureHandler(msgArray, ctx) {
        // ctx.log("Failed to send to Sumo");
        if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
            ctx.done("TaskConsumer failedmessages: " + sumoClient.messagesFailed);
        }
    }
    function successHandler(ctx) {
        // ctx.log('Successfully sent to Sumo', serviceBusTask);
        if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
            if (sumoClient.messagesFailed > 0) {
                ctx.done("TaskConsumer failedmessages: " + sumoClient.messagesFailed);
            } else {
                ctx.log('Sent ' + sumoClient.messagesAttempted + ' data to Sumo. Exit now.');
                ctx.done();
            }
        }
    }

    var sumoClient = new sumoHttp.SumoClient(options, context, failureHandler, successHandler);
    messageHandler(serviceBusTask, context, process.env.APPSETTING_StorageAcccountConnectionString, sumoClient);

}
function timetriggerhandler(context, timetrigger) {

    if (timetrigger.isPastDue) {
        context.log("timetriggerhandler running late");
    }
    var serviceBusService = servicebus.createServiceBusService(process.env.APPSETTING_TaskQueueConnectionString);
    serviceBusService.receiveQueueMessage(process.env.APPSETTING_TASKQUEUE_NAME + '/$DeadLetterQueue', {isPeekLock: true}, function (error, lockedMessage) {
        if (!error) {
            // Message received and locked and try to resend
            var options = {
                urlString: process.env.APPSETTING_SumoLogEndpoint,
                metadata: {},
                MaxAttempts: 3,
                RetryInterval: 3000,
                compress_data: true,
                clientHeader: "dlqblobreader-azure-function"
            };
            var serviceBusTask = JSON.parse(lockedMessage.body);
            function failureHandler(msgArray, ctx) {
                ctx.log("Failed to send to Sumo");
                if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                    ctx.done("TaskConsumer failedmessages: " + sumoClient.messagesFailed);
                }
            }
            function successHandler(ctx) {
                ctx.log('Successfully sent to Sumo');
                if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                    if (sumoClient.messagesFailed > 0) {
                        ctx.done("DLQTaskConsumer failedmessages: " + sumoClient.messagesFailed);
                    } else {
                        ctx.log('Sent ' + sumoClient.messagesAttempted + ' data to Sumo. Exit now.');
                        serviceBusService.deleteMessage(lockedMessage, function (deleteError) {
                            if (!deleteError) {
                                context.log("sent and deleted");
                                ctx.done();
                            } else {
                                ctx.done("Messages Sent but failed delete from DeadLetterQueue");
                            }
                        });
                    }
                }
            }
            var sumoClient = new sumoHttp.SumoClient(options, context, failureHandler, successHandler);
            messageHandler(serviceBusTask, context, process.env.APPSETTING_StorageAcccountConnectionString, sumoClient);

        } else {
            if(typeof error === 'string' && new RegExp("\\b" + "No messages" + "\\b", "gi").test(error)) {
                context.log("No messages Found exiting.");
                context.done();
            } else {
                context.log("Error in reading messages from DLQ: ", error, typeof(error));
                context.done(error);
            }
        }

    });
}
module.exports = function (context, triggerData) {
    //var options ={ 'urlString':process.env.APPSETTING_SumoSelfEventHubBadEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true};
   if (triggerData.isPastDue === undefined) {
        servicebushandler(context, triggerData);
    } else {
        timetriggerhandler(context, triggerData);
    }
};
