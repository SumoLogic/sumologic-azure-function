///////////////////////////////////////////////////////////////////////////////////
//           Function to read from an Azure EventHubs to SumoLogic               //
///////////////////////////////////////////////////////////////////////////////////

var sumoHttp = require('./sumoclient');
var dataTransformer = require('./datatransformer');
var storage = require('azure-storage');
var storageManagementClient = require('azure-arm-storage');
var MsRest = require('ms-rest-azure');
var servicebus = require('azure-sb');
var DEFAULT_CSV_SEPARATOR = ",";
var MAX_CHUNK_SIZE = 1024;
var JSON_BLOB_HEAD_BYTES = 12;
var JSON_BLOB_TAIL_BYTES = 2;
var https = require('https');

function csvToArray(strData, strDelimiter) {
    strDelimiter = (strDelimiter || ",");
    var objPattern = new RegExp(
        (
            "(\\" + strDelimiter + "|\\r?\\n|\\r|^)" +           // Delimiters.
            "(?:\"([^\"]*(?:\"\"[^\"]*)*)\"|" +                  // Quoted fields.
            "([^\"\\" + strDelimiter + "\\r\\n]*))"              // Standard fields.
        ),
        "gi"
    );
    var arrData = [[]];
    var arrMatches = null;
    var strMatchedValue;
    var strMatchedDelimiter;
    while (arrMatches = objPattern.exec(strData)) {
        strMatchedDelimiter = arrMatches[1];
        if (strMatchedDelimiter.length && strMatchedDelimiter !== strDelimiter) {
            arrData.push([]);
        }

        if (arrMatches[2]) {
            strMatchedValue = arrMatches[2].replace( //unescape any double quotes.
                new RegExp("\"\"", "g"),
                "\""
                );
        } else {
            strMatchedValue = arrMatches[3]; // We found a non-quoted value.
        }
        arrData[arrData.length - 1].push(strMatchedValue);
    }
    return arrData;
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

function getHeaderRecursively(headertext, task, blobService, context) {

    return new Promise(function (resolve, reject) {
        getData(task, blobService, context).then(function (text) {
            headertext += text;
            var onlyheadertext = hasAllHeaders(headertext);
            var bytesOffset = MAX_CHUNK_SIZE;
            if (onlyheadertext) {
                var csvHeaders = csvToArray(onlyheadertext, DEFAULT_CSV_SEPARATOR);
                if (csvHeaders && csvHeaders[0].length > 0) {
                    resolve(csvHeaders[0]);
                } else {
                    reject("Error in csvToArray parsing: " + csvHeaders);
                }
            } else {
                task.startByte = task.endByte + 1;
                task.endByte = task.startByte + bytesOffset - 1;
                getHeaderRecursively(headertext, task, blobService, context).then(function (headers) {
                    resolve(headers);
                }).catch(function (err) {
                    reject(err);
                });
            }
        }).catch(function (err) {
            reject(err);
        });
    });

}

function getcsvHeader(containerName, blobName, context, blobService) {
    // Todo optimize to avoid multiple request
    var bytesOffset = MAX_CHUNK_SIZE;
    var task = {
        containerName: containerName,
        blobName: blobName,
        startByte: 0,
        endByte: bytesOffset - 1
    };

    return getHeaderRecursively("", task, blobService, context);
}

function csvHandler(msgtext, headers) {
    var messages = csvToArray(msgtext, DEFAULT_CSV_SEPARATOR);
    var messageArray = [];
    if (headers.length > 0 && messages.length > 0 && messages[0].length > 0 && headers[0] === messages[0][0]) {
        messages = messages.slice(1); //removing header row
    }
    messages.forEach(function (row) {
        if (row.length === headers.length) {
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
    // it's assumed that json is well formed {},{}
    var jsonArray = [];

    msg = msg.trim().replace(/(^,)|(,$)/g, ""); //removing trailing spaces,newlines and leftover commas
    jsonArray = JSON.parse("[" + msg + "]");
    return jsonArray;
}

function blobHandler(msg) {
    // it's assumed that .blob files contains json separated by \n
    //https://docs.microsoft.com/en-us/azure/application-insights/app-insights-export-telemetry

    var jsonArray = [];
    msg = msg.trim().replace(/(^,)|(,$)/g, ""); //removing trailing spaces,newlines and leftover commas
    msg = msg.replace(/(\r?\n|\r)/g, ",");
    jsonArray = JSON.parse("[" + msg + "]");
    return jsonArray;
}

function logHandler(msg) {
    return [msg];
}

function getData(task, blobService, context) {
    // Todo support for chunk reading(if range is large)
    // valid offset status code 206 (Partial Content).
    // invalid offset status code 416 (Requested Range Not Satisfiable)

    var containerName = task.containerName;
    var blobName = task.blobName;
    var options = {rangeStart: task.startByte, rangeEnd: task.endByte};

    return new Promise(function (resolve, reject) {
        blobService.getBlobToText(containerName, blobName, options, function (err, blobContent, blob) {
            if (err) {
                reject(err);
            } else {
                resolve(blobContent);
            }
        });
    });

}

function getToken() {
    var options = {msiEndpoint: process.env.MSI_ENDPOINT, msiSecret: process.env.MSI_SECRET};
    return new Promise(function (resolve, reject) {
        MsRest.loginWithAppServiceMSI(options, function (err, tokenResponse) {
            if (err) {
                reject(err);
            } else {
                resolve(tokenResponse);
            }
        });
    });
}

function getStorageAccountAccessKey(task) {
    return getToken().then(function (credentials) {
        var storagecli = new storageManagementClient(
          credentials,
          task.subscriptionId
        );
        return storagecli.storageAccounts.listKeys(task.resourceGroupName, task.storageName).then(function (resp) {
            return resp.keys[0].value;
        });
    });
}

function getBlockBlobService(context, task) {

    return getStorageAccountAccessKey(task).then(function (accountKey) {
        var blobService = storage.createBlobService(task.storageName, accountKey);
        return blobService;
    });

}

function messageHandler(serviceBusTask, context, sumoClient) {
    var file_ext = serviceBusTask.blobName.split(".").pop();
    var msghandler = {"log": logHandler, "csv": csvHandler, "json": jsonHandler, "blob": blobHandler};
    if (!(file_ext in msghandler)) {
        context.done("Unknown file extension: " + file_ext + " for blob: " + serviceBusTask.blobName);
        return;
    }
    if (file_ext === "json") {
        // because in json first block and last block remain as it is and azure service adds new block in 2nd last pos
        if (serviceBusTask.endByte < JSON_BLOB_HEAD_BYTES + JSON_BLOB_TAIL_BYTES) {
            context.done(); //rejecting first commit when no data is there data will always be atleast HEAD_BYTES+DATA_BYTES+TAIL_BYTES
            return;
        }
        serviceBusTask.endByte -= JSON_BLOB_TAIL_BYTES;
        if (serviceBusTask.startByte <= JSON_BLOB_HEAD_BYTES) {
            serviceBusTask.startByte = JSON_BLOB_HEAD_BYTES;
        } else {
            serviceBusTask.startByte -= JSON_BLOB_TAIL_BYTES;
        }

    }
    getBlockBlobService(context, serviceBusTask).then(function (blobService) {
        return getData(serviceBusTask, blobService, context).then(function (msg) {
            context.log("Sucessfully downloaded blob %s %d %d", serviceBusTask.blobName, serviceBusTask.startByte, serviceBusTask.endByte);
            var messageArray;
            if (file_ext === "csv") {
                getcsvHeader(serviceBusTask.containerName, serviceBusTask.blobName, context, blobService).then(function (headers) {
                    context.log("Received headers %d", headers.length);
                    messageArray = msghandler[file_ext](msg, headers);
                    // context.log("Transformed data %s", JSON.stringify(messageArray));
                    messageArray.forEach(function (msg) {
                        sumoClient.addData(msg);
                    });
                    sumoClient.flushAll();
                }).catch(function (err) {
                    context.log("Error in creating json from csv " + err);
                    context.done(err);
                });
            } else {
                messageArray = msghandler[file_ext](msg);
                messageArray.forEach(function (msg) {
                    sumoClient.addData(msg);
                });
                sumoClient.flushAll();
            }
        });
    }).catch(function (err) {
        context.log("Error in messageHandler: Failed to send blob %s %d %d", serviceBusTask.blobName, serviceBusTask.startByte, serviceBusTask.endByte);
        context.done(err);
    });
}

function servicebushandler(context, serviceBusTask) {
    var sumoClient;
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

    sumoClient = new sumoHttp.SumoClient(options, context, failureHandler, successHandler);
    messageHandler(serviceBusTask, context, sumoClient);

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
            var sumoClient;
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
            sumoClient = new sumoHttp.SumoClient(options, context, failureHandler, successHandler);
            messageHandler(serviceBusTask, context, sumoClient);

        } else {
            if (typeof error === 'string' && new RegExp("\\b" + "No messages" + "\\b", "gi").test(error)) {
                context.log(error);
                context.done();
            } else {
                context.log("Error in reading messages from DLQ: ", error, typeof(error));
                context.done(error);
            }
        }

    });
}
module.exports = function (context, triggerData) {

   if (triggerData.isPastDue === undefined) {
        servicebushandler(context, triggerData);
    } else {
        timetriggerhandler(context, triggerData);
    }
};
