///////////////////////////////////////////////////////////////////////////////////
//           Function to read from an Azure EventHubs to SumoLogic               //
///////////////////////////////////////////////////////////////////////////////////

var sumoHttp = require('./sumoclient');
var { ContainerClient } = require("@azure/storage-blob");
var { DefaultAzureCredential } = require("@azure/identity");
var { AbortController } = require("@azure/abort-controller");
var { ServiceBusClient } = require("@azure/service-bus");
var DEFAULT_CSV_SEPARATOR = ",";
var MAX_CHUNK_SIZE = 1024;
var JSON_BLOB_HEAD_BYTES = 12;
var JSON_BLOB_TAIL_BYTES = 2;

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

function getcsvHeader(containerName, blobName, blobService, context) {
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

function nsgLogsHandler(jsonArray) {
    var splitted_tuples, eventsArr = [];
    jsonArray.forEach(function (record) {
        version = record.properties.Version;
        record.properties.flows.forEach(function (rule) {
            rule.flows.forEach(function (flow) {
                flow.flowTuples.forEach(function (tuple) {
                    col = tuple.split(",");
                    event = {
                        time: col[0], // this should be epoch time
                        sys_id: record.systemId,
                        category: record.category,
                        resource_id: record.resourceId,
                        event_name: record.operationName,
                        rule_name: rule.rule,
                        mac: flow.mac,
                        src_ip: col[1],
                        dest_IP: col[2],
                        src_port: col[3],
                        dest_port: col[4],
                        protocol: col[5],
                        traffic_destination: col[6],
                        "traffic_a/d": col[7],
                        version: version,
                        flow_state: null,
                        num_packets_sent_src_to_dest: null,
                        bytes_sent_src_to_dest: null,
                        num_packets_sent_dest_to_src: null,
                        bytes_sent_dest_to_src: null
                        // nsg_name:
                        // resource_group_name:
                    }
                    if (version === 2) {
                        event.flow_state = (col[8] === "" || col[8] === undefined) ?  null : col[8];
                        event.num_packets_sent_src_to_dest = (col[9] === "" || col[9] === undefined) ?  null : col[9];
                        event.bytes_sent_src_to_dest = (col[10] === "" || col[10] === undefined) ?  null : col[10];
                        event.num_packets_sent_dest_to_src = (col[11] === "" || col[11] === undefined) ?  null : col[11];
                        event.bytes_sent_dest_to_src = (col[12] === "" || col[12] === undefined) ?  null : col[12];
                    }
                    eventsArr.push(event);
                })
            })
        })
    });
    return eventsArr;
}

function jsonHandler(msg) {
    // it's assumed that json is well formed {},{}
    var jsonArray = [];
    msg = JSON.stringify(msg)
    msg = msg.trim().replace(/(^,)|(,$)/g, ""); //removing trailing spaces,newlines and leftover commas
    jsonArray = JSON.parse("[" + msg + "]");
    jsonArray = (jsonArray.length > 0 && jsonArray[0].category === "NetworkSecurityGroupFlowEvent") ? nsgLogsHandler(jsonArray) : jsonArray;
    return jsonArray;
}

function blobHandler(msg) {
    // it's assumed that .blob files contains json separated by \n
    //https://docs.microsoft.com/en-us/azure/application-insights/app-insights-export-telemetry
    
    var jsonArray = [];
    msg = msg.replace(/\0/g, '');
    msg = msg.replace(/(\r?\n|\r)/g, ",");
    msg = msg.trim().replace(/(^,+)|(,+$)/g, ""); //removing trailing spaces,newlines and leftover commas
    jsonArray = JSON.parse("[" + msg + "]");
    return jsonArray;
}

function logHandler(msg) {
    return [msg];
}

function getData(task, blockBlobClient, context) {
    // Todo support for chunk reading(if range is large)
    // valid offset status code 206 (Partial Content).
    // invalid offset status code 416 (Requested Range Not Satisfiable)
    context.log("Inside get data function:");
    return new Promise(async function (resolve, reject) {
        try {
            var buffer = Buffer.alloc(4 * 1024 * 1024);
            await blockBlobClient.downloadToBuffer(buffer, task.startByte, (task.endByte - task.startByte + 1) , {
            abortSignal: AbortController.timeout(30 * 60 * 1000),
            blockSize: 4 * 1024 * 1024,
            concurrency: 1
            });
            resolve(buffer.toString());
        } catch (err) {
            reject(err);
        }
        // context.log(buffer.toString());
    })};

function getBlockBlobService(context, task) {
    return new Promise(function (resolve, reject) {
    try{
        context.log("Inside Block Blob Service")
        var tokenCredential = new DefaultAzureCredential();
        var containerClient = new ContainerClient(
            `https://${task.storageName}.blob.core.windows.net/${task.containerName}`,
            tokenCredential
        );
        var blockBlobClient = containerClient.getBlockBlobClient(task.blobName);
        resolve(blockBlobClient);
        } catch (err){
            reject(err);
        }
    })};

function messageHandler(serviceBusTask, context, sumoClient) {
    var file_ext = serviceBusTask.blobName.split(".").pop();
    if (file_ext == serviceBusTask.blobName) {
        file_ext = "log";
    }
    context.log("Reached Here nearest to blob handler");
    var msghandler = {"log": logHandler, "csv": csvHandler, "json": jsonHandler, "blob": blobHandler};
    context.log("message handler called: ")
    context.log(msghandler);
    if (!(file_ext in msghandler)) {
        context.log("Error in messageHandler: Unknown file extension - " + file_ext + " for blob: " + serviceBusTask.blobName)
        context.done();
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
                getcsvHeader(serviceBusTask.containerName, serviceBusTask.blobName, blobService, context).then(function (headers) {
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
        if(err.statusCode === 404) {
            context.log("Error in messageHandler: blob file doesn't exist  %s %d %d", serviceBusTask.blobName, serviceBusTask.startByte, serviceBusTask.endByte);
            context.done()
        } else {
            context.log("Error in messageHandler: Failed to send blob %s %d %d", serviceBusTask.blobName, serviceBusTask.startByte, serviceBusTask.endByte);
            context.done(err);
        }

    });
}

function setSourceCategory(serviceBusTask, options) {

    options.metadata = options.metadata || {};
    options.metadata["name"]= serviceBusTask.storageName + "/" + serviceBusTask.containerName + "/" + serviceBusTask.blobName;
    // options.metadata["category"] = <custom source category>
}

function servicebushandler(context, serviceBusTask) {
    var sumoClient;

    var options = {
        urlString: process.env.APPSETTING_SumoLogEndpoint,
        MaxAttempts: 3,
        RetryInterval: 3000,
        compress_data: true,
        clientHeader: "blobreader-azure-function"
    };
    setSourceCategory(serviceBusTask, options);
    function failureHandler(msgArray, ctx) {
        ctx.log("Failed to send to Sumo");
        if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
            ctx.done("TaskConsumer failedmessages: " + sumoClient.messagesFailed);
        }
    }
    function successHandler(ctx) {
        ctx.log('Successfully sent to Sumo', serviceBusTask);
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
    context.log("Reached inside ServiceBus Handler")
    messageHandler(serviceBusTask, context, sumoClient);

}

async function timetriggerhandler(context, timetrigger) {

    if (timetrigger.isPastDue) {
        context.log("timetriggerhandler running late");
    }
    try{
        var sbClient = new ServiceBusClient(process.env.APPSETTING_TaskQueueConnectionString);
        var queueReceiver = sbClient.createReceiver(process.env.APPSETTING_TASKQUEUE_NAME,{ subQueueType: "deadLetter", receiveMode: "peekLock" });
    }catch(err){
        context.log(err);
        context.done("Failed to create service bus client and receiver");
    }
    try {
        var messages = await queueReceiver.receiveMessages(1, {
            maxWaitTimeInMs: 60 * 1000,
        });
        if (!messages.length) {
            context.log("No more messages to receive");
            await queueReceiver.close();
            await sbClient.close();
            context.done();
            return;
        }
        var myJSON = JSON.stringify(messages[0].body);
        var serviceBusTask = JSON.parse(myJSON);
        // Message received and locked and try to resend
        var options = {
            urlString: process.env.APPSETTING_SumoLogEndpoint,
            MaxAttempts: 3,
            RetryInterval: 3000,
            compress_data: true,
            clientHeader: "dlqblobreader-azure-function"
        };
        setSourceCategory(serviceBusTask, options);
        var sumoClient;
        async function failureHandler(msgArray, ctx) {
            ctx.log("Failed to send to Sumo");
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                await queueReceiver.close();
                await sbClient.close();
                ctx.done("TaskConsumer failedmessages: " + sumoClient.messagesFailed);
            }
        }
        async function successHandler(ctx) {
            ctx.log('Successfully sent to Sumo');
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                //TODO: Test Scenario for combination of successful and failed requests
                if (sumoClient.messagesFailed > 0) {
                    await queueReceiver.close();
                    await sbClient.close();
                    ctx.done("DLQTaskConsumer failedmessages: " + sumoClient.messagesFailed);
                } else {
                    ctx.log('Sent ' + sumoClient.messagesAttempted + ' data to Sumo. Exit now.');
                    try{
                        await queueReceiver.completeMessage(messages[0]);
                    }catch(err){
                        await queueReceiver.close();
                        await sbClient.close();
                        if (!err) {
                            context.log("sent and deleted");
                            ctx.done();
                        } else {
                            context.log(err)
                            ctx.done("Messages Sent but failed delete from DeadLetterQueue");
                        }
                    }
                }
            }
        }
        sumoClient = new sumoHttp.SumoClient(options, context, failureHandler, successHandler);
        messageHandler(serviceBusTask, context, sumoClient);
      }catch(error){
        await queueReceiver.close();
        await sbClient.close();
        if (typeof error === 'string' && new RegExp("\\b" + "No messages" + "\\b", "gi").test(error)) {
            context.log(error);
            context.done();
        } else {
            context.log("Error in reading messages from DLQ: ", error, typeof(error));
            context.done(error);
        }
      }
    }

module.exports = function (context, triggerData) {
    if (triggerData.isPastDue === undefined) {
        servicebushandler(context, triggerData);
    } else {
        timetriggerhandler(context, triggerData);
    }
};