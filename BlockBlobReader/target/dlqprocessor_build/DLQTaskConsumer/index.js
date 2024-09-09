///////////////////////////////////////////////////////////////////////////////////
//           Function to read from an Azure EventHubs to SumoLogic               //
///////////////////////////////////////////////////////////////////////////////////

var sumoHttp = require('./sumoclient');
var { ContainerClient } = require("@azure/storage-blob");
var { DefaultAzureCredential } = require("@azure/identity");
const { TableClient } = require("@azure/data-tables");
var { AbortController } = require("@azure/abort-controller");
var { ServiceBusClient } = require("@azure/service-bus");
var DEFAULT_CSV_SEPARATOR = ",";
var MAX_CHUNK_SIZE = 1024;
var JSON_BLOB_HEAD_BYTES = 12;
var JSON_BLOB_TAIL_BYTES = 2;
const azureTableClient = TableClient.fromConnectionString(process.env.AzureWebJobsStorage, "FileOffsetMap");

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

function csvHandler(context,msgtext, headers) {
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

/*
    return index of first time when pattern matches the string
 */
function regexIndexOf(string, regex, startpos) {
    var indexOf = string.substring(startpos || 0).search(regex);
    return (indexOf >= 0) ? (indexOf + (startpos || 0)) : indexOf;
}

/*
    return index of last time when pattern matches the string
 */
function regexLastIndexOf(string, regex, startpos) {
    // https://stackoverflow.com/questions/19445994/javascript-string-search-for-regex-starting-at-the-end-of-the-string
    var stringToWorkWith = string.substring(startpos, string.length);
    var match = stringToWorkWith.match(regex);
    return match ? string.lastIndexOf(match.slice(-1)) : -1;
}

/*
    returns array of json by removing unparseable prefix and suffix in data
 */
function getParseableJsonArray(data, context) {

    let logRegex = '{\\s*\"time\"\:'; // starting regex for nsg logs
    let defaultEncoding = "utf8";
    let orginalDatalength = data.length;
    // If the byte sequence in the buffer data is not valid according to the provided encoding, then it is replaced by the default replacement character i.e. U+FFFD.
    // return -1 if not found
    let firstIdx = regexIndexOf(data, logRegex);
    let lastIndex = regexLastIndexOf(data, logRegex, firstIdx + 1);

    // data.substring method extracts the characters in a string between "start" and "end", not including "end" itself.
    let prefix = data.substring(0, firstIdx);
    // in case only one time string
    if (lastIndex === -1 && data.length > 0) {
        lastIndex = data.length;
    }
    let suffix = data.substring(lastIndex, data.length);
    if (suffix.length > 0) {
        try {
            JSON.parse(suffix.trim());
            lastIndex = data.length;
        } catch (error) {
            context.log.error(`Failed to parse the JSON last chunk. Ignoring suffix: ${suffix.trim()}, error: ${error}`);
        }
    }

    // ideally ignoredprefixLen should always be 0. it will be dropped for files which are updated
    context.log.verbose(`Ignoring log prefix length: ${Buffer.byteLength(prefix, defaultEncoding)} suffix length: ${Buffer.byteLength(data.substring(lastIndex, data.length), defaultEncoding)}`);

    // data with both prefix and suffix removed
    data = data.substring(firstIdx, lastIndex);
    let dataLenParsed = Buffer.byteLength(prefix + data, defaultEncoding);
    data = data.trim().replace(/(^,)|(,$)/g, ""); //removing trailing spaces,newlines and leftover commas

    try {
        var jsonArray = JSON.parse("[" + data + "]");
        context.log.verbose(`Successfully parsed Json! datalength: ${data.length} orginalDatalength: ${orginalDatalength} dataLenParsed: ${dataLenParsed}`)
        return [jsonArray, dataLenParsed, true];
    } catch(error) {
        context.log.error(`Failed to parse the JSON after removing prefix/suffix  Error: ${error} firstIdx: ${firstIdx} lastIndex: ${lastIndex} prefix: ${prefix} datastart: ${data.substring(0,10)} dataend: ${data.substring(data.length-10,data.length)} orginalDatalength: ${orginalDatalength} dataLenParsed: ${dataLenParsed}`);
        return [[data], dataLenParsed, false];
    }
}

function getRowKey(metadata) {
    var storageName =  metadata.url.split("//").pop().split(".")[0];
    var arr = metadata.url.split('/').slice(3);
    var keyArr = [storageName];
    Array.prototype.push.apply(keyArr, arr);
    // key cannot be greater than 1KB or 1024 bytes;
    var rowKey = keyArr.join("-");
    return rowKey.substr(0,Math.min(1024, rowKey.length)).replace(/^-+|-+$/g, '');
}

async function setAppendBlobOffset(context, serviceBusTask, newOffset) {

    try {
        let rowKey = getRowKey(serviceBusTask);
        // Todo: this should be atomic update if other request decreases offset it shouldn't allow
        context.log.verbose("Attempting to update offset row: %s from: %d to: %d", rowKey, serviceBusTask.startByte, newOffset);
        let entity = {
            offset: { type: "Int64", value: String(newOffset) },
            // In a scenario where the entity could have been deleted (archived) by appendblob because of large queueing time so to avoid error in insertOrMerge Entity we include rest of the fields like storageName,containerName etc.
            partitionKey: serviceBusTask.containerName,
            rowKey: rowKey,
            blobName: serviceBusTask.blobName,
            containerName: serviceBusTask.containerName,
            storageName: serviceBusTask.storageName
        }
        var updateResult = await azureTableClient.updateEntity(entity, "Merge");
        context.log.verbose("Updated offset result: %s row: %s from: %d to: %d", JSON.stringify(updateResult), rowKey, serviceBusTask.startByte, newOffset);
    } catch (error) {
        context.log.error(`Error - Failed to update OffsetMap table, error: ${JSON.stringify(error)},  rowKey: ${rowKey}, newOffset: ${newOffset}`)
    }
}

async function nsgLogsHandler(context, msg, serviceBusTask) {

    var jsonArray = [];
    msg = msg.trim().replace(/(^,)|(,$)/g, ""); //removing trailing spaces,newlines and leftover commas

    try {
        jsonArray = JSON.parse("[" + msg + "]");
    } catch(err) {
        let response = getParseableJsonArray(msg, context, serviceBusTask);
        jsonArray = response[0];
        let is_success = response[2];
        let newOffset = response[1] + serviceBusTask.startByte;
        if (is_success) {
            await setAppendBlobOffset(context, serviceBusTask, newOffset);
        } else {
            return jsonArray;
        }

    }

    var eventsArr = [];
    jsonArray.forEach(function (record) {
        let version = record.properties.Version;
        record.properties.flows.forEach(function (rule) {
            rule.flows.forEach(function (flow) {
                flow.flowTuples.forEach(function (tuple) {
                    let col = tuple.split(",");
                    let event = {
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

function jsonHandler(context,msg) {
    // it's assumed that json is well formed {},{}
    var jsonArray = [];
    msg = JSON.stringify(msg)
    msg = msg.trim().replace(/(^,)|(,$)/g, ""); //removing trailing spaces,newlines and leftover commas
    jsonArray = JSON.parse("[" + msg + "]");
    return jsonArray;
}

function blobHandler(context,msg) {
    // it's assumed that .blob files contains json separated by \n
    //https://docs.microsoft.com/en-us/azure/application-insights/app-insights-export-telemetry

    var jsonArray = [];
    msg = msg.replace(/\0/g, '');
    msg = msg.replace(/(\r?\n|\r)/g, ",");
    msg = msg.trim().replace(/(^,+)|(,+$)/g, ""); //removing trailing spaces,newlines and leftover commas
    jsonArray = JSON.parse("[" + msg + "]");
    return jsonArray;
}

function logHandler(context,msg) {
    return [msg];
}

function getData(task, blockBlobClient, context) {
    // Todo support for chunk reading(if range is large)
    // valid offset status code 206 (Partial Content).
    // invalid offset status code 416 (Requested Range Not Satisfiable)
    //context.log("Inside get data function:");
    return new Promise(async function (resolve, reject) {
        try {
            var buffer = Buffer.alloc(task.endByte - task.startByte + 1);
            await blockBlobClient.downloadToBuffer(buffer, task.startByte, (task.endByte - task.startByte + 1) , {
            abortSignal: AbortController.timeout(30 * 60 * 1000),
            blockSize: 4 * 1024 * 1024,
            concurrency: 1
            });
            resolve(buffer.toString());
        } catch (err) {
            reject(err);
        }
    })};

function getBlockBlobService(context, task) {
    return new Promise(function (resolve, reject) {
    try{
        //context.log("Inside Block Blob Service")
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
    var msghandler = {"log": logHandler, "csv": csvHandler, "json": jsonHandler, "blob": blobHandler, "nsg": nsgLogsHandler};
    if (!(file_ext in msghandler)) {
        context.log.error("Error in messageHandler: Unknown file extension - " + file_ext + " for blob: " + serviceBusTask.blobName);
        context.done();
        return;
    }
    if ((file_ext === "json") && (serviceBusTask.containerName === "insights-logs-networksecuritygroupflowevent")) {
        // because in json first block and last block remain as it is and azure service adds new block in 2nd last pos
        if ((serviceBusTask.endByte < JSON_BLOB_HEAD_BYTES + JSON_BLOB_TAIL_BYTES) || (serviceBusTask.endByte == serviceBusTask.startByte)) {
            context.done(); //rejecting first commit when no data is there data will always be atleast HEAD_BYTES+DATA_BYTES+TAIL_BYTES
            return;
        }
        serviceBusTask.endByte -= JSON_BLOB_TAIL_BYTES;
        if (serviceBusTask.startByte <= JSON_BLOB_HEAD_BYTES + JSON_BLOB_TAIL_BYTES) {
            serviceBusTask.startByte = JSON_BLOB_HEAD_BYTES;
        } else {
            serviceBusTask.startByte -= 1; //to remove comma before json object
        }
        file_ext = "nsg";
    }
    getBlockBlobService(context, serviceBusTask).then(function (blobService) {
        return getData(serviceBusTask, blobService, context).then(async function (msg) {
            context.log("Sucessfully downloaded blob %s %d %d", serviceBusTask.blobName, serviceBusTask.startByte, serviceBusTask.endByte);
            var messageArray;
            if (file_ext === "csv") {
                return getcsvHeader(serviceBusTask.containerName, serviceBusTask.blobName, blobService, context).then(function (headers) {
                    context.log("Received headers %d", headers.length);
                    messageArray = csvHandler(context,msg, headers);
                    // context.log("Transformed data %s", JSON.stringify(messageArray));
                    messageArray.forEach(function (msg) {
                        sumoClient.addData(msg);
                    });
                    sumoClient.flushAll();
                }).catch(function (err) {
                    context.log.error("Error in creating json from csv.");
                    context.done(err);
                });
            } else {
                if (file_ext == "nsg") {
                    messageArray = await nsgLogsHandler(context, msg, serviceBusTask);
                } else {
                    messageArray = msghandler[file_ext](context,msg);
                }
                messageArray.forEach(function (msg) {
                    sumoClient.addData(msg);
                });
                sumoClient.flushAll();
            }
        });
    }).catch(function (err) {
        if(err.statusCode === 404) {
            context.log.error("Error in messageHandler: blob file doesn't exist " + serviceBusTask.blobName + " " + serviceBusTask.startByte + " " +serviceBusTask.endByte);
            context.done()
        } else {
            context.log.error("Error in messageHandler: Failed to send blob " + serviceBusTask.blobName + " " + serviceBusTask.startByte + " " +serviceBusTask.endByte + " err: " + err);
            context.done(err);
        }

    });
}

/**
 * Truncates the given string if it exceeds 128 characters and creates a new string.
 * If the string is within the limit, returns the original string.
 * @param {string} data - The original data.
 * @returns {string} - The new string, truncated if necessary.
 */
function checkAndTruncate(data) {

    const maxLength = 1024;

    // Check if the string length exceeds the maximum length
    if (data.length > maxLength) {
        // Truncate the data, taking the first 60 characters, adding "..." in between, and taking the last 60 characters
        return data.substring(0, 60) + "..." + data.substring(data.length - 60);
    } else {
        // If the data is within the limit, return the original data
        return data;
    }
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
    // make sure to add custom fileds in HTTP source in sumologic portal: https://help.sumologic.com/docs/manage/fields/#collector-and-source-fields, otherwise these fileds will be dropped.
    let customFields = {}; // { "containername": serviceBusTask.containerName, "storagename": serviceBusTask.storageName };
    if (customFields) {
        let customFieldsArr = []
        Object.keys(customFields).map(function (key, index) {
            customFieldsArr.push(key.toString() + "=" + customFields[key].toString());
        });
        options.metadata["sourceFields"] = customFieldsArr.join();
    }
    options.metadata["sourceHost"] = checkAndTruncate(`${serviceBusTask.storageName}/${serviceBusTask.containerName}`);
    // context.log(serviceBusTask.blobName, serviceBusTask.storageName,serviceBusTask.containerName);
    // options.metadata["sourceCategory"] = "custom_source_category";
    options.metadata["sourceName"] = checkAndTruncate(serviceBusTask.blobName);
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
        ctx.log.error(`Failed to send to Sumo`);
        if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
            ctx.done("TaskConsumer failedmessages: " + sumoClient.messagesFailed);
        }
    }
    function successHandler(ctx) {
        if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
            if (sumoClient.messagesFailed > 0) {
                ctx.log.error(`Failed to send few messages to Sumo`)
                ctx.done("TaskConsumer failedmessages: " + sumoClient.messagesFailed);
            } else {
                ctx.log(`Successfully sent to Sumo, Exiting now.`);
                ctx.done();
            }
        }
    }

    sumoClient = new sumoHttp.SumoClient(options, context, failureHandler, successHandler);
    //context.log("Reached inside ServiceBus Handler")
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
        context.log.error("Failed to create service bus client and receiver");
        context.done(err);
        return;
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
            ctx.log.error("Failed to send to Sumo");
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                await queueReceiver.close();
                await sbClient.close();
                ctx.done("TaskConsumer failedmessages: " + sumoClient.messagesFailed);
            }
        }
        async function successHandler(ctx) {
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                //TODO: Test Scenario for combination of successful and failed requests
                if (sumoClient.messagesFailed > 0) {
                    await queueReceiver.close();
                    await sbClient.close();
                    ctx.log.error('Failed to send few messages to Sumo')
                    ctx.done("DLQTaskConsumer failedmessages: " + sumoClient.messagesFailed);
                } else {
                    ctx.log('Successfully sent to Sumo, Exiting now.');
                    try{
                        await queueReceiver.completeMessage(messages[0]);
                        ctx.log("Successfully deleted message from DLQ.");
                    } catch(err){
                        ctx.log.error(`Failed to delete message from DLQ. error: ${error}`);
                    }
                    await queueReceiver.close();
                    await sbClient.close();
                    ctx.done();
                }
            }
        }
        sumoClient = new sumoHttp.SumoClient(options, context, failureHandler, successHandler);
        messageHandler(serviceBusTask, context, sumoClient);
      } catch(error){
        await queueReceiver.close();
        await sbClient.close();
        if (typeof error === 'string' && new RegExp("\\b" + "No messages" + "\\b", "gi").test(error)) {
            context.log.error(error);
            context.done();
        } else {
            context.log.error("Error in reading messages from DLQ");
            context.done(error);
        }
      }
    }

module.exports = function (context, triggerData) {
    // triggerData = {
    //     "blobName": "blob_fixtures.json",
    //     "containerName": "insights-logs-networksecuritygroupflowevent",
    //     "endByte": 2617,
    //     "resourceGroupName": "testsumosa250624004409",
    //     "startByte": 0,
    //     "storageName": "testsa250624004409",
    //     "subscriptionId": "c088dc46-d692-42ad-a4b6-9a542d28ad2a",
    //     "url": "https://testsa250624004409.blob.core.windows.net/insights-logs-networksecuritygroupflowevent/blob_fixtures.json"
    // };
    if (triggerData.isPastDue === undefined) {
        servicebushandler(context, triggerData);
    } else {
        timetriggerhandler(context, triggerData);
    }
};
