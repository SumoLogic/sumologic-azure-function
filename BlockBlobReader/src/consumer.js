//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Function to read from an Azure Storage Account by consuming task from Service Bus and send data to SumoLogic //
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
var tableService = storage.createTableService(process.env.APPSETTING_AzureWebJobsStorage);
/**
 * @param  {} strData
 * @param  {} strDelimiter
 *
 * converts multiline string from a csv file to array of rows
 */
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
/**
 * @param  {} headertext
 * @param  {} task
 * @param  {} blobService
 * @param  {} context
 *
 * Extracts the header from the start of the csv file
 */
function getHeaderRecursively(headertext, task, blobService, context) {

    return new Promise(function (resolve, reject) {
        getData(task, blobService, context).then(function (r) {
            var text = r[0];
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
/**
 * @param  {} msgtext
 * @param  {} headers
 * Handler for CSV to JSON log conversion
 */
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
/**
 * @param  {} jsonArray
 * Handler for NSG Flow logs to JSON supports version 1 and version 2 both
 */
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
                        event.flow_state = (col[8] === "" || col[8] === undefined) ? null : col[8];
                        event.num_packets_sent_src_to_dest = (col[9] === "" || col[9] === undefined) ? null : col[9];
                        event.bytes_sent_src_to_dest = (col[10] === "" || col[10] === undefined) ? null : col[10];
                        event.num_packets_sent_dest_to_src = (col[11] === "" || col[11] === undefined) ? null : col[11];
                        event.bytes_sent_dest_to_src = (col[12] === "" || col[12] === undefined) ? null : col[12];
                    }
                    eventsArr.push(event);
                })
            })
        })
    });
    return eventsArr;
}
/**
 * @param  {} msg
 *
 * Handler for extracting multiple json objects from the middle of the json array(from a file present in storage account)
 */
function jsonHandler(msg) {
    // it's assumed that json is well formed {},{}
    var jsonArray = [];

    msg = msg.trim().replace(/(^,)|(,$)/g, ""); //removing trailing spaces,newlines and leftover commas
    jsonArray = JSON.parse("[" + msg + "]");
    jsonArray = (jsonArray.length > 0 && jsonArray[0].category === "NetworkSecurityGroupFlowEvent") ? nsgLogsHandler(jsonArray) : jsonArray;
    return jsonArray;
}
/**
 * @param  {} msg
 * Handler for json line format where every line is a json object
 */
function blobHandler(context, msg, serviceBusTask) {
    // it's assumed that .blob files contains json separated by \n
    //https://docs.microsoft.com/en-us/azure/application-insights/app-insights-export-telemetry

    let jsonArray = [];
    msg = msg.trim().replace(/(^,)|(,$)/g, ""); //removing trailing spaces,newlines and leftover commas

    try {
        jsonArray = JSON.parse("[" + msg.replace(/}\r?\n{/g,  "},{") + "]");
    } catch(e) {
        context.log("JSON ParseException in blobHandler");
        context.log(e, msg);
        // removing unparsed prefix and suffix
        let start_idx = msg.indexOf('{');
        let last_idx = msg.lastIndexOf('}');
        let submsg = msg.substr(start_idx, last_idx+1-start_idx); // prefix & suffix removed
        try {
            jsonArray = JSON.parse("[" + msg.replace(/}\r?\n{/g,  "},{") + "]");
            let suffixlen = msg.length - (li+1);
            contentDownloaded -= suffixlen;
        } catch(e) {
            context.log("JSON ParseException in blobHandler for rowKey: " + serviceBusTask.rowKey + " with submsg ", start_idx, last_idx, msg.substr(0,start_idx), msg.substr(last_idx+1));
            // will try to ingest the whole block
            jsonArray = [msg];
        }
    }
    return jsonArray;
}

function logHandler(msg) {
    return [msg];
}

function getUpdatedEntity(task, endByte) {
    //a single entity group transaction is limited to 100 entities. Also, the entire payload of the transaction may not exceed 4MB
    var entGen = storage.TableUtilities.entityGenerator;
    // RowKey/Partition key cannot contain "/"
    // sets the offset updatedate done(releases the enque lock)
    var entity = {
        done: entGen.Boolean(false),
        updatedate: entGen.DateTime((new Date()).toISOString()),
        offset: entGen.Int64(endByte),
        // In a scenario where the entity could have been deleted (archived) by appendblob because of large queueing time so to avoid error in insertOrMerge Entity we include rest of the fields like storageName,containerName etc.
        PartitionKey: entGen.String(task.containerName),
        RowKey: entGen.String(task.rowKey),
        blobName: entGen.String(task.blobName),
        containerName: entGen.String(task.containerName),
        storageName: entGen.String(task.storageName),
        blobType: entGen.String(task.blobType),
        resourceGroupName: entGen.String(task.resourceGroupName),
        subscriptionId: entGen.String(task.subscriptionId)
    };
    if (contentDownloaded > 0) {
        entity["senddate"] = entGen.DateTime((new Date()).toISOString())
    }
    return entity;
}
/**
 * @param  {} task
 * @param  {} blobResult
 *
 * updates the offset in FileOffsetMap table for append blob file rows after the data has been sent to sumo
 */
var contentDownloaded = 0;
function setAppendBlobOffset(task) {
    return new Promise(function (resolve, reject) {
        // Todo: this should be atomic update if other request decreases offset it shouldn't allow
        var newOffset = parseInt(task.startByte, 10) + contentDownloaded;
        entity = getUpdatedEntity(task, newOffset);
        //using merge to preserve eventdate
        tableService.insertOrMergeEntity(process.env.APPSETTING_TABLE_NAME, entity, function (error, result, response) {
            if (!error) {
                resolve(response);
            } else {
                reject(error);
            }
        });
    });
}
/**
 * @param  {} task
 * @param  {} blobService
 * @param  {} context
 *
 * fetching ranged data from a file in storage account
 */
function getData(task, blobService, context) {
    // Todo support for chunk reading(if range is large)
    // valid offset status code 206 (Partial Content).
    // invalid offset status code 416 (Requested Range Not Satisfiable)

    var containerName = task.containerName;
    var blobName = task.blobName;
    var options = { rangeStart: task.startByte };
    if (task.endByte) {
        options.rangeEnd = task.endByte;
    }

    return new Promise(function (resolve, reject) {
        blobService.getBlobToText(containerName, blobName, options, function (err, blobContent, blobResult) {
            if (err) {
                context.log("Error in fetching!")
                reject(err);
            } else {
                resolve([blobContent, blobResult]);
            }
        });
    });

}

function getToken(context, task) {
    var options = { msiEndpoint: process.env.MSI_ENDPOINT, msiSecret: process.env.MSI_SECRET };
    return new Promise(function (resolve, reject) {
        MsRest.loginWithAppServiceMSI(options, function (err, tokenResponse) {
            if (err) {
                context.log("MSI_REST_TOKEN", err, task);
                reject(err);
            } else {
                resolve(tokenResponse);
            }
        });
    });
}

function getStorageAccountAccessKey(context, task) {
    return getToken(context, task).then(function (credentials) {
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

    return getStorageAccountAccessKey(context, task).then(function (accountKey) {
        var blobService = storage.createBlobService(task.storageName, accountKey);
        return blobService;
    });

}

function messageHandler(serviceBusTask, context, sumoClient) {

    var file_ext = String(serviceBusTask.blobName).split(".").pop();
    var msghandler;

    if (file_ext.indexOf("log") >= 0 || file_ext == serviceBusTask.blobName) {
        msghandler = logHandler;
    } else if (file_ext === "csv") {
        msghandler = csvHandler
    } else if (file_ext === "blob") {
        msghandler = blobHandler
    } else if (file_ext === "json" && serviceBusTask.blobType === "AppendBlob") {
        // jsonline format where every line is a json object
        msghandler = blobHandler
    } else if (file_ext === "json" && serviceBusTask.blobType !== "AppendBlob") {
        // JSON format ie array of json objects is not supported for appendblobs
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

    } else {
        context.done("Unknown file extension: " + file_ext + " for blob: " + serviceBusTask.blobName);
        return;
    }

    getBlockBlobService(context, serviceBusTask).then(function (blobService) {
        context.log("fetching blob %s %d %d", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte);
        return getData(serviceBusTask, blobService, context).then(function (r) {
            var msg = r[0];
            var resp = r[1];
            context.log("Sucessfully downloaded contentLength: ", resp.contentLength);
            if (!serviceBusTask.endByte) {
                // setting this to increment offset in setAppendBlobOffset
                contentDownloaded = parseInt(resp.contentLength, 10);
            }
            var messageArray;
            if (file_ext === "csv") {
                getcsvHeader(serviceBusTask.containerName, serviceBusTask.blobName, context, blobService).then(function (headers) {
                    context.log("Received headers %d", headers.length);
                    messageArray = csvHandler(msg, headers);
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
                if (file_ext === "json" && serviceBusTask.blobType === "AppendBlob") {
                    messageArray = blobHandler(context, msg, serviceBusTask);
                } else {
                    messageArray = msghandler(msg);
                }

                messageArray.forEach(function (msg) {
                    sumoClient.addData(msg);
                });
                sumoClient.flushAll();
            }
        });
    }).catch(function (err) {
        if (serviceBusTask.blobType === "AppendBlob" && err.statusCode === 416 && err.code === "InvalidRange") {
            // here in case of appendblob data may not exists after startByte
            context.log("offset is already at the end startbyte: %d of blob: %s ", serviceBusTask.startByte, serviceBusTask.blobName);

            return setAppendBlobOffset(serviceBusTask).then(function (res) {
                var newOffset = parseInt(serviceBusTask.startByte, 10) + contentDownloaded;
                ctx.log("Successfully updated OffsetMap for row: " + serviceBusTask.rowKey +  " table to : " + newOffset + " from: " + serviceBusTask.startByte);
                ctx.done();
            }).catch(function (error) {
                ctx.log("Failed to update OffsetMap table: ", error, serviceBusTask)
                ctx.done(error);
            });
        } else {
            context.log("Error in messageHandler:  blob %s %d %d", serviceBusTask.blobName, serviceBusTask.startByte, serviceBusTask.endByte);
            context.done(err);
        }

    });
}
/**
 * @param {} servisBusTask
 * @param {} options
 *
 * This functions is used to route the logs/metrics to custom source categories based on the serviceBusTask attributes and also to add other metadata.
 * metadata.sourceName attribute sets the source name
 * metadata.sourceHost attribute sets the source host
 * metadata.sourceCategory attribute sets the source category
 */
function setSourceCategory(serviceBusTask, options) {
    options.metadata = options.metadata || {};
    let customFields = {};
    // var sourcecategory = "<default source category>";
    // switch(serviceBusTask.storageName) {
    //     case "<your storage account name1>":
    //         switch(serviceBusTask.containerName) {
    //             case "<your container name within the storage account>":
    //                 sourcecategory = "<your source category where you want to route logs from within container>"
    //                 break;
    //         }
    //         break;
    //     case "<your storage account name2>":
    //         switch(serviceBusTask.containerName) {
    //             case "<your container name within the storage account>":
    //                 sourcecategory = "<your source category where you want to route logs from within container>"
    //                 break;
    //         }
    //         break;
    // }
    // customFields["testcustomfield"] = <add your custom field value. Do not forget to add your field name(Ex testcustomfield) in the source otherwise it will be dropped>;
    // options.metadata["sourceCategory"] =  sourcecategory;
    if (customFields) {
        let customFieldsArr = []
        Object.keys(customFields).map(function(key, index) {
            customFieldsArr.push(key.toString() + "=" + customFields[key].toString());
        });
        options.metadata["sourceFields"] = customFieldsArr.join();
    }

    options.metadata["sourceName"]= serviceBusTask.blobName;

}
/**
 * @param  {} context
 * @param  {} serviceBusTask
 *
 * This is invoked when the function is triggered by Service bug Trigger
 * It consumes the tasks from service bus.
 */
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
                if (serviceBusTask.blobType === "AppendBlob") {
                    return setAppendBlobOffset(serviceBusTask).then(function (res) {
                        var newOffset = parseInt(serviceBusTask.startByte, 10) + contentDownloaded;
                        ctx.log("Successfully updated OffsetMap for row: " + serviceBusTask.rowKey +  " table to : " + newOffset + " from: " + serviceBusTask.startByte);
                        ctx.done();
                    }).catch(function (error) {
                        ctx.log("Failed to update OffsetMap table: ", error, serviceBusTask)
                        ctx.done(error);
                    });
                } else {
                    ctx.done();
                }
            }
        }
    }

    sumoClient = new sumoHttp.SumoClient(options, context, failureHandler, successHandler);
    messageHandler(serviceBusTask, context, sumoClient);

}
/**
 * @param  {} context
 * @param  {} timetrigger
 * This is invoked when the function is triggered by Time trigger
 * It consumes tasks from Dead letter queue of the service bus
 */
function timetriggerhandler(context, timetrigger) {

    if (timetrigger.isPastDue) {
        context.log("timetriggerhandler running late");
    }
    var serviceBusService = servicebus.createServiceBusService(process.env.APPSETTING_TaskQueueConnectionString);
    serviceBusService.receiveQueueMessage(process.env.APPSETTING_TASKQUEUE_NAME + '/$DeadLetterQueue', { isPeekLock: true }, function (error, lockedMessage) {
        if (!error) {
            var serviceBusTask = JSON.parse(lockedMessage.body);
            // Message received and locked and try to resend
            var options = {
                urlString: process.env.APPSETTING_SumoLogEndpoint,
                metadata: getSourceCategory(serviceBusTask),
                MaxAttempts: 3,
                RetryInterval: 3000,
                compress_data: true,
                clientHeader: "dlqblobreader-azure-function"
            };

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

                        if (serviceBusTask.blobType === "AppendBlob") {
                            setAppendBlobOffset(serviceBusTask).then(function (res) {
                                var newOffset = parseInt(serviceBusTask.startByte, 10) + contentDownloaded;
                                ctx.log("Successfully updated OffsetMap for row: " + serviceBusTask.rowKey +  " table to : " + newOffset + " from: " + serviceBusTask.startByte);
                                serviceBusService.deleteMessage(lockedMessage, function (deleteError) {
                                    if (!deleteError) {
                                        ctx.log("sent and deleted");
                                        ctx.done();
                                    } else {
                                        ctx.done("Messages Sent but failed delete from DeadLetterQueue");
                                    }
                                });
                            }).catch(function (error) {
                                ctx.log("Failed to update OffsetMap table: ", error, serviceBusTask)
                                ctx.done(error);
                            });
                        } else {
                            serviceBusService.deleteMessage(lockedMessage, function (deleteError) {
                                if (!deleteError) {
                                    ctx.log("sent and deleted");
                                    ctx.done();
                                } else {
                                    ctx.done("Messages Sent but failed delete from DeadLetterQueue");
                                }
                            });
                        }
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
                context.log("Error in reading messages from DLQ: ", error, typeof (error));
                context.done(error);
            }
        }

    });
}

module.exports = function (context, triggerData) {
    contentDownloaded = 0;
    if (triggerData.isPastDue === undefined) {
        servicebushandler(context, triggerData);
    } else {
        timetriggerhandler(context, triggerData);
    }
};
