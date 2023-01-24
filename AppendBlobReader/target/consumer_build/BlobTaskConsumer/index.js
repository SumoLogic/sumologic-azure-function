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
var DLQMessage = null;
var contentDownloaded = 0;
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

function setAppendBlobOffset(context, serviceBusTask, dataLenSent) {
    return new Promise(function (resolve, reject) {
        // Todo: this should be atomic update if other request decreases offset it shouldn't allow
        var newOffset = parseInt(serviceBusTask.startByte, 10) + dataLenSent;
        // context.log("Attempting to update offset row: %s to: %d from: %d", serviceBusTask.rowKey, newOffset, serviceBusTask.startByte);
        entity = getUpdatedEntity(serviceBusTask, newOffset);
        //using merge to preserve eventdate
        tableService.mergeEntity(process.env.APPSETTING_TABLE_NAME, entity, function (error, result, response) {
            if (!error) {
                if (serviceBusTask.startByte === newOffset) {
                    context.log("Successfully updated Lock  for Table row: " + serviceBusTask.rowKey +  " Offset remains the same : " + newOffset);
                } else {
                    context.log("Successfully updated OffsetMap Table row: " + serviceBusTask.rowKey +  " table to : " + newOffset + " from: " + serviceBusTask.startByte);
                }

                resolve(response);
            } else if (error.code === "ResourceNotFound" && error.statusCode === 404) {
                context.log("Already Archived AppendBlob File with RowKey: " + serviceBusTask.rowKey);
                resolve(response)
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
var lastTokenGenTime = null;
var refreshTokenDuratoninMin = 60;
var cachedTokenResponse = null;


function isRefreshTokenDurationExceeded() {
    if (!lastTokenGenTime) {
        return true;
    }
    var currentTimestamp = new Date().getTime();
    return ((currentTimestamp - lastTokenGenTime)/(1000 * 60)) >= refreshTokenDuratoninMin ? true : false;
}

/**
- * @param  {} task
- * @param  {} context
- *
- * fetching token and caching it for refreshTokenDuratoninMin
- */
function getCachedToken(context, task) {
    if (!cachedTokenResponse || isRefreshTokenDurationExceeded()) {
        context.log("Regenerating token at: %s", new Date().toISOString());
        return getToken(context, task).then(function(tokenResponse) {
            lastTokenGenTime = new Date().getTime();
            cachedTokenResponse = tokenResponse;
            return cachedTokenResponse;
        })
    } else {
        return new Promise(function (resolve, reject) {
            var timeRemainingToRefreshToken = ((new Date()).getTime() - lastTokenGenTime)/(1000 * 60)
            // context.log("Using cached token timeRemainingToRefreshToken: %d", timeRemainingToRefreshToken);
            resolve(cachedTokenResponse);
        });
    }
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


var lastAccountKeyGenTime = {};
var refreshAccountKeyDuratoninMin = 60;
var cachedAccountKeyResponse = {};

function isRefreshAccountKeyDurationExceeded(task) {
    if (!lastAccountKeyGenTime[getStorageAccountCacheKey(task)]) {
        return true;
    }
    var currentTimestamp = new Date().getTime();
    return ((currentTimestamp - lastAccountKeyGenTime[getStorageAccountCacheKey(task)])/(1000 * 60)) >= refreshAccountKeyDuratoninMin ? true : false;
}

function getStorageAccountCacheKey(task) {
    return task.resourceGroupName + "-" + task.storageName;
}
/**
 * @param  {} task
 * @param  {} context
 *
 * fetching account key and caching it for refreshAccountKeyDuratoninMin
 */
function getCachedAccountKey(context, task) {
    if (!cachedAccountKeyResponse[getStorageAccountCacheKey(task)] || isRefreshAccountKeyDurationExceeded(task)) {
        // context.log("Regenerating accountKey for %s at: %s ", task.rowKey, new Date().toISOString());
        return getStorageAccountAccessKey(context, task).then(function(accountKey) {
            lastAccountKeyGenTime[getStorageAccountCacheKey(task)] = new Date().getTime();
            cachedAccountKeyResponse[getStorageAccountCacheKey(task)] = accountKey;
            context.log("Regenerated accountKey for %s at: %s ", task.rowKey, new Date().toISOString());
            return accountKey;
        });

    } else {
        return new Promise(function (resolve, reject) {
            var timeRemainingToRefreshToken = (lastAccountKeyGenTime[getStorageAccountCacheKey(task)] +  (refreshAccountKeyDuratoninMin * 1000 * 60) - (new Date()).getTime())/(1000 * 60);
            context.log("Using cached accountKey for %s timeRemainingToRefreshToken: %d", task.rowKey, timeRemainingToRefreshToken);
            resolve(cachedAccountKeyResponse[getStorageAccountCacheKey(task)]);
        });
    }
}

function getStorageAccountAccessKey(context, task) {

    return getCachedToken(context, task).then(function(credentials) {
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

    return getCachedAccountKey(context, task).then(function (accountKey) {
        var blobService = storage.createBlobService(task.storageName, accountKey);
        return blobService;
    });

}

function releaseLockfromOffsetTable(context, serviceBusTask, dataLenSent) {
    var curdataLenSent = dataLenSent || contentDownloaded;
    if (serviceBusTask.blobType === "AppendBlob") {
        return setAppendBlobOffset(context, serviceBusTask, curdataLenSent).catch(function (error) {
            // not failing with error because log will automatically released by appendblob
            context.log("Error - Failed to update OffsetMap table: ", error, serviceBusTask, curdataLenSent)
        });
    } else {
        return new Promise(function (resolve, reject) {resolve();});
    }
}

function releaseMessagefromDLQ(context, serviceBusTask) {
    return new Promise(function (resolve, reject) {
        if (DLQMessage) {
            var serviceBusService = servicebus.createServiceBusService(process.env.APPSETTING_TaskQueueConnectionString);
            serviceBusService.deleteMessage(DLQMessage, function (deleteError) {
                if (!deleteError) {
                    context.log("Deleted DeadLetterQueue Message");
                } else {
                    context.log("Failed to delete from DeadLetterQueue", deleteError);
                }
                resolve();
            });
        } else {
            resolve();
        }
    });
}

function sendToSumoBlocking(chunk, sendOptions, context, isText) {

    return new Promise(function(resolve, reject) {

        function failureHandler(msgArray, ctx) {
            reject();
        }
        function successHandler(ctx) {
            // ctx.log('Sent ' + sumoClient.messagesAttempted + ' data to Sumo.');
            resolve();
        }
        let sumoClient = new sumoHttp.SumoClient(sendOptions, context, failureHandler, successHandler);
        if (!isText) {
            // Default encoding is UTF-8
            let data = chunk.toString('utf8');
            sumoClient.addData(data);
        } else {
            sumoClient.addData(chunk);
        }

        sumoClient.flushAll();
    });

}


function setAppendBlobBatchSize(serviceBusTask) {

    var batchSize = serviceBusTask.batchSize;
    if (serviceBusTask.containerName === "cct-prod-logs") {
        batchSize = 180 * 1024 * 1024;
    } else if (serviceBusTask.containerName === "onboard-prod-applogs") {
        batchSize = 30 * 1024 * 1024;
    }

    return batchSize;
}

function getSumoEndpoint(serviceBusTask) {
    var file_ext = String(serviceBusTask.blobName).split(".").pop();
    var endpoint = process.env.APPSETTING_SumoLogEndpoint;
    // You can also change change sumo logic endpoint if you have multiple sources

    return endpoint;
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
    var match = string.match(regex);
    return  match ? string.lastIndexOf(match.slice(-1)) : -1;
}


/*  Funciton to use boundary regex for azure storage accounts to avoid split issue & multiple single event issue */
function getBoundaryRegex(serviceBusTask) {
    //this boundary regex is matching for cct, llev, msgdir logs
    let logRegex = '\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}';
    // global is necessary if using regex.exec
    // https://stackoverflow.com/questions/31969913/why-does-this-regexp-exec-cause-an-infinite-loop
    var file_ext = String(serviceBusTask.blobName).split(".").pop();
    if (file_ext === "json") {
        logRegex = '\{\\s+\"time\"\:\\s+\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}';
    }
    if (serviceBusTask.storageName === "mue1supportakspocsa" || serviceBusTask.storageName === "mue1supportaksdevsa" || serviceBusTask.storageName === "muw1nortonaksintsa" || serviceBusTask.storageName === "muw1supportaksstgsa" || serviceBusTask.storageName === "muw1supportaksprodsa" || serviceBusTask.storageName === "mue2supportaksprodsa" || serviceBusTask.storageName === "muw1supportakscoresa") {
        if (file_ext === "log") {
            logRegex = '\{\"\@timestamp\"\:\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}';
        } else if(file_ext === "json") {
            if(serviceBusTask.containerName === "insights-logs-kube-audit") {
                logRegex = '\{\\s+\"operationName\"\:\\s+\"';
            }
            else {
                logRegex = '\{\\s+\"attrs\"\:\\s+\"';
            }
        }
    } else if (serviceBusTask.storageName === "muw1bitfunctionslogssa" || serviceBusTask.storageName === "mue1bitfunctionslogssa") {
        logRegex = '\{\\s+\"time\"\:\\s+\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}';;
    } else if ((serviceBusTask.storageName === "muw1olpolpadminccatsa01" || serviceBusTask.storageName === "mue2olpolpadminccatsa01" || serviceBusTask.storageName === "muw1olpolpadminsa01") && serviceBusTask.containerName === "insights-logs-appservicehttplogs") {
        logRegex = '\{\\s+\"category\"\:\\s+\"';
    }

    return new RegExp(logRegex, "gim");
}

/*
    Removes the prefix and suffix matching the boundary.
    For example if data = "prefixwithnodate<date><msg><date><msg><date><incompletemsg>"
    then it will remove prefixwithnodate and <date><incompletemsg>.
    It will iterate over index of <date> and divide the remaining string to chunks
     of maximum 1MB size.
    datalenSent is set to bytelength of prefixwithnodate<date><msg><date><msg><date>
    so that in next invocation it will pick up from <date><incompletemsg>
 */
function decodeDataChunks(context, dataBytesBuffer, serviceBusTask) {

    var dataChunks = [];
    let defaultEncoding = "utf8";
    // If the byte sequence in the buffer data is not valid according to the provided encoding, then it is replaced by the default replacement character i.e. U+FFFD.
    var data = dataBytesBuffer.toString(defaultEncoding);
    // remove prefix before first date
    // remove suffix after last date
    let logRegex = getBoundaryRegex(serviceBusTask);

    // returm -1 if not found
    let firstIdx = regexIndexOf(data, logRegex);
    let lastIndex = regexLastIndexOf(data, logRegex, firstIdx+1);

    // data.substring method extracts the characters in a string between "start" and "end", not including "end" itself.
    let prefix = data.substring(0,firstIdx);
    // in case only one time string
    if (lastIndex === -1 && data.length > 0) {
        lastIndex = data.length;
    }
    let suffix = data.substring(lastIndex, data.length);

    try {
        // if last chunk is parseable then make lastIndex = data.length
        if (suffix.length > 0) {
            JSON.parse(suffix)
            lastIndex = data.length;
        }
    } catch(e) {
        context.log("last chunk not json parseable so ignoring", suffix, lastIndex,  e);
    }
    // ideally ignoredprefixLen should always be 0. it will be dropped for existing files
    // for new files offset will always start from date
    var ignoredprefixLen = Buffer.byteLength(prefix, defaultEncoding);
    // data with both prefix and suffix removed
    data = data.substring(firstIdx, lastIndex);
    // can't use matchAll since it's available only after version > 12
    let startpos = 0;
    let maxChunkSize = 1 * 1024 * 1024; // 1 MB
    while((match = logRegex.exec(data)) !== null) {

        if (match.index - startpos >= maxChunkSize) {
            dataChunks.push(data.substring(startpos, match.index));
            // context.log("new chunk %d %d", startpos, match.index);
            startpos = match.index;
        }
    }
    // pushing the remaining chunk
    let lastChunk = data.substring(startpos, data.length);
    if (lastChunk.length > 0) {
        dataChunks.push(lastChunk);
    }

    context.log(`decodeDataChunks rowKey: ${serviceBusTask.rowKey} numChunks: ${dataChunks.length} ignoredprefixLen: ${ignoredprefixLen} suffixLen: ${Buffer.byteLength(suffix, defaultEncoding)} dataLenTobeSent: ${Buffer.byteLength(data, defaultEncoding)}`);
    return [ignoredprefixLen, dataChunks];
}
/*
    Creates a promise chain for each of the chunk recieved from decodeDataChunks
    It increments
 */
function sendDataToSumoUsingSplitHandler(context, dataBytesBuffer, sendOptions, serviceBusTask) {


    var results = decodeDataChunks(context, dataBytesBuffer, serviceBusTask);
    var ignoredprefixLen = results[0];
    var dataChunks = results[1];
    var numChunksSent = 0;
    var dataLenSent = 0;
    return new Promise(function(resolve, reject) {

        let promiseChain = Promise.resolve();
        const makeNextPromise = (chunk) => () => {
            return sendToSumoBlocking(chunk, sendOptions, context, true).then(function() {
                if ((numChunksSent >= 10 && numChunksSent % 10 === 0) || numChunksSent <= 2) {
                    context.log("sent chunks: ", numChunksSent);
                }
                numChunksSent += 1;
                dataLenSent += Buffer.byteLength(chunk);
            });
        };
        for(var i = 0; i < dataChunks.length; i++) {
            promiseChain = promiseChain.then(makeNextPromise(dataChunks[i]));
        }
        return promiseChain.catch(function(err) {
            context.log(`Error in sendDataToSumoUsingSplitHandler blob: ${serviceBusTask.rowKey} prefix: ${ignoredprefixLen} Sent ${dataLenSent} bytes of data. numChunksSent ${numChunksSent}`)
            resolve(dataLenSent+ignoredprefixLen);
        }).then(function() {
            if (dataChunks.length === 0) {
                context.log(`No chunks to send ${serviceBusTask.rowKey}`);
            } else if (numChunksSent === dataChunks.length) {
                if (numChunksSent === dataChunks.length) {
                    context.log(`All chunks sucessfully sent to sumo blob ${serviceBusTask.rowKey} prefix: ${ignoredprefixLen} Sent ${dataLenSent} bytes of data. numChunksSent ${numChunksSent}`);
                }
            }
            resolve(dataLenSent+ignoredprefixLen);
        });

    });

}
/*
    First all the data(of length batchsize) is fetched and then sequentially sends the data by splitting into 1 MB chunks and removing splitted chunks in boundary
 */
function errorHandler(err, serviceBusTask, context) {
    let discardError = false;
    let errMsg = (err !== undefined ? err.toString(): "");
    if (typeof errMsg === 'string' && (errMsg.indexOf("MSIAppServiceTokenCredentials.parseTokenResponse") >= 0 || errMsg.indexOf("SyntaxError: Unexpected end of JSON input") >= 0)) {
        context.log("Error in appendBlobStreamMessageHandlerv2 MSI Toke Error ignored: blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        discardError = true;
    } else if (err !== undefined && err.statusCode === 416 && err.code === "InvalidRange") {
        // here in case of appendblob data may not exists after startByte
        context.log("offset is already at the end startbyte: %d of blob: %s Exit now!", serviceBusTask.startByte, serviceBusTask.rowKey);
        discardError = true;
    } else if (err !== undefined && (err.statusCode === 503 || err.statusCode === 500 || err.statusCode == 429)) {
        context.log("Error in appendBlobStreamMessageHandlerv2 Potential throttling scenario: blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        discardError = true;
    } else if (err !== undefined && (err.code === "ContainerNotFound" || err.code === "BlobNotFound" || err.statusCode == 404)) {
        // sometimes it may happen that the file/container gets deleted
        context.log("Error in appendBlobStreamMessageHandlerv2 File location doesn't exists:  blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        discardError = true;
    } else if (err !== undefined && (err.code === "ECONNREFUSED" || err.code === "ECONNRESET" || err.code === "ETIMEDOUT")) {
        context.log("Error in appendBlobStreamMessageHandlerv2 Connection Refused Error: blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        discardError = true;

    } else {
        context.log("Error in appendBlobStreamMessageHandlerv2 Unknown error blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
    }
    return discardError;
}

function archiveIngestedFile(serviceBusTask, context) {
    return tableService.deleteEntity(process.env.APPSETTING_TABLE_NAME, {
        PartitionKey: serviceBusTask.containerName,
        RowKey: serviceBusTask.rowKey
    }, function(err, resp) {
        if (!err) {
            context.log("Archived non existing file", serviceBusTask.rowKey);
        } else {
            context.log("Error in appendBlobStreamMessageHandlerv2: not able to delete table row", serviceBusTask.rowKey, err);
        }
        context.done()
    });
}

function appendBlobStreamMessageHandlerv2(context, serviceBusTask) {

    var file_ext = String(serviceBusTask.blobName).split(".").pop();
    if ((file_ext.indexOf("log") >= 0 || file_ext == serviceBusTask.blobName) || file_ext === "json") {
        // context.log("file extension: %s", file_ext);
    } else {
        context.done("Unknown file extension: " + file_ext + " for blob: " + serviceBusTask.rowKey);
    }
    var sendOptions = {
        urlString: getSumoEndpoint(serviceBusTask),
        MaxAttempts: 3,
        RetryInterval: 3000,
        compress_data: true,
        clientHeader: "blobreader-azure-function"
    };
    setSourceCategory(serviceBusTask, sendOptions, context);

    return getBlockBlobService(context, serviceBusTask).then(function (blobService) {
        context.log("fetching blob %s %d %d", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte);
        var numChunks = 0;
        let batchSize = setAppendBlobBatchSize(serviceBusTask); // default batch size from sdk code
        context.log("setting batchsize", batchSize);
        var dataLen = 0;
        var dataChunks=[];
        let options = {
            "rangeStart": serviceBusTask.startByte,
            "rangeEnd": serviceBusTask.startByte + batchSize - 1,
            "skipSizeCheck": false, // if true it will call getblobProperties and makes decision to download full(returns _getBlobToStream) or download in range (returns _getBlobToRangeStream)
            // "speedSummary" // Todo: check whether it will print something
            // "parallelOperationThreadCount": 1 // default is 1 for appendblobToText and 5 for others
            // timeoutIntervalInMs server timeout
            // clientRequestTimeoutInMs client timeout
        };
        let readStream = blobService.createReadStream(serviceBusTask.containerName, serviceBusTask.blobName, options, (err, res) => {
            if(err) {
                context.log('Error in appendBlobStreamMessageHandlerv2 Failed to create readStream', res, err, err.statusCode);
            }
        });

        readStream.on('data', (chunk, range) => {
            // Todo: returns 4 MB chunks we may need to break it to 1MB
            if ((numChunks >= 10 && numChunks % 10 === 0) || numChunks <= 2) {
                context.log(`Received ${chunk.length} bytes of data. numChunks ${numChunks} range: ${JSON.stringify(range)}`);
            }
            dataLen += chunk.length;
            numChunks += 1;
            // https://github.com/Azure/azure-sdk-for-js/blob/master/sdk/storage/storage-blob/samples/javascript/basic.js#L73
            dataChunks.push(chunk instanceof Buffer ? chunk : Buffer.from(chunk));

        });

        readStream.on('end', function(res) {
            context.log(`Finished Fetching numChunks: ${numChunks} dataLen: ${dataLen} rowKey: ${serviceBusTask.rowKey}`);
            return sendDataToSumoUsingSplitHandler(context, Buffer.concat(dataChunks), sendOptions, serviceBusTask).then(function(dataLenSent) {
                contentDownloaded = dataLenSent;
                return releaseLockfromOffsetTable(context, serviceBusTask, dataLenSent).then(function() {
                        context.done();
                    });
            }).catch(function(err) {
                context.log(`Error in sendDataToSumoUsingSplitHandler: ${serviceBusTask.rowKey} err ${err}`)
                context.done();
            });
        });

        readStream.on('error', function(err) {
            // Todo: Test error cases for fetching
        return sendDataToSumoUsingSplitHandler(context, Buffer.concat(dataChunks), sendOptions, serviceBusTask).then(function(dataLenSent) {
                contentDownloaded = dataLenSent;
                let discardError = errorHandler(err, serviceBusTask, context);
                if (err !== undefined && (err.code === "BlobNotFound" || err.statusCode == 404)) {
                    // delete the entry from table storage
                    return archiveIngestedFile(serviceBusTask, context);
                } else {
                    return releaseLockfromOffsetTable(context, serviceBusTask,dataLenSent).then(function() {
                        if (discardError) {
                            context.done()
                        } else {
                            // after 1 hr lock automatically releases
                            context.done(err);
                        }
                    });
                }

            }).catch(function(err) {
                context.log(`Error in sendDataToSumoUsingSplitHandler inside OnError: ${serviceBusTask.rowKey} err ${err}`)
                return releaseLockfromOffsetTable(context, serviceBusTask).then(function() {
                        context.done();
                });
            });

        });

    }).catch(function (err) {
        let discardError = errorHandler(err, serviceBusTask, context);
        return releaseLockfromOffsetTable(context, serviceBusTask).then(function() {
            if (discardError) {
                context.done();
            } else {
                // after 1 hr lock automatically releases
                context.done(err);
            }
        });
    });
}



function messageHandler(serviceBusTask, context, sumoClient) {

    var file_ext = String(serviceBusTask.blobName).split(".").pop();
    var msghandler;

    if (file_ext.indexOf("log") >= 0 || file_ext == serviceBusTask.blobName) {
        msghandler = logHandler;
    } else if (file_ext === "csv") {
        msghandler = csvHandler;
    } else if (file_ext === "blob") {
        msghandler = blobHandler;
    } else if (file_ext === "json" && serviceBusTask.blobType === "AppendBlob") {
        // jsonline format where every line is a json object
        msghandler = blobHandler;
    } else if (file_ext === "json" && serviceBusTask.blobType !== "AppendBlob") {
        // JSON format ie array of json objects is not supported for appendblobs
        // because in json first block and last block remain as it is and azure service adds new block in 2nd last pos
        if (serviceBusTask.endByte < JSON_BLOB_HEAD_BYTES + JSON_BLOB_TAIL_BYTES) {
            //rejecting first commit when no data is there data will always be atleast HEAD_BYTES+DATA_BYTES+TAIL_BYTES
            return releaseMessagefromDLQ(context, serviceBusTask).then(function(){
                context.done();
            });
        }
        serviceBusTask.endByte -= JSON_BLOB_TAIL_BYTES;
        if (serviceBusTask.startByte <= JSON_BLOB_HEAD_BYTES) {
            serviceBusTask.startByte = JSON_BLOB_HEAD_BYTES;
        } else {
            serviceBusTask.startByte -= JSON_BLOB_TAIL_BYTES;
        }
        msghandler = jsonHandler;

    } else {
        // releasing message so that it doesn't get stuck in DLQ
        return releaseMessagefromDLQ(context, serviceBusTask).then(function(){
            context.done("Unknown file extension: " + file_ext + " for blob: " + serviceBusTask.rowKey);
        });
    }

    return getBlockBlobService(context, serviceBusTask).then(function (blobService) {
        context.log("fetching blob %s %d %d", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte);
        return getData(serviceBusTask, blobService, context).then(function (r) {
            var msg = r[0];
            var resp = r[1];
            context.log("Sucessfully downloaded contentLength: ", resp.contentLength);

            var messageArray;
            if (file_ext === "csv") {
                return getcsvHeader(serviceBusTask.containerName, serviceBusTask.blobName, context, blobService).then(function (headers) {
                    context.log("Received headers %d", headers.length);
                    messageArray = csvHandler(msg, headers);
                    // context.log("Transformed data %s", JSON.stringify(messageArray));
                    messageArray.forEach(function (msg) {
                        sumoClient.addData(msg);
                    });
                    sumoClient.flushAll();
                }).catch(function (err) {
                    context.log("Error in creating json from csv " + err);
                    return releaseLockfromOffsetTable(context, serviceBusTask).then(function() {
                        return releaseMessagefromDLQ(context, serviceBusTask).then(function() {
                            context.done(err);
                        });
                    });
                });
            } else {
                if ((file_ext === "json" && serviceBusTask.blobType === "AppendBlob") || (file_ext === "blob" )) {
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


        let discardError = false;
        if (err !== undefined && err.statusCode === 416 && err.code === "InvalidRange") {
            // here in case of appendblob data may not exists after startByte
            context.log("offset is already at the end startbyte: %d of blob: %s Exit now!", serviceBusTask.startByte, serviceBusTask.rowKey);
            discardError = true;
        } else if(err.statusCode == 404){
            discardError = true;
        } else if (err !== undefined && (err.statusCode === 503 || err.statusCode === 500 || err.statusCode == 429)) {
            context.log("Potential throttling scenario: blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        } else if (err !== undefined && (err.code === "ContainerNotFound" || err.code === "BlobNotFound")) {
            // sometimes it may happen that the file/container gets deleted
            context.log("File location doesn't exists:  blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
            discardError = true;
        } else if (err !== undefined && (err.code === "ECONNREFUSED")) {
            context.log("Connection Refused Error: blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        } else {
            context.log("Error in messageHandler:  blob %s %d %d %d %s %s", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code, err);
        }
        return releaseLockfromOffsetTable(context, serviceBusTask).then(function() {
            return releaseMessagefromDLQ(context, serviceBusTask).then(function() {
                if (discardError) {
                    context.done();
                } else {
                    // after 1 hr lock automatically releases
                    context.done(err);
                }
            });
        });

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
function setSourceCategory(serviceBusTask, options,context) {
    options.metadata = options.metadata || {};
    let customFields = {"containername":serviceBusTask.containerName,"storagename":serviceBusTask.storageName};
    let sourcecategory = "azure_br_logs";
    //var source = "azure_blobstorage"
    if (customFields) {
        let customFieldsArr = []
        Object.keys(customFields).map(function(key, index) {
            customFieldsArr.push(key.toString() + "=" + customFields[key].toString());
        });
        options.metadata["sourceFields"] = customFieldsArr.join();
    }
    //options.metadata["sourceHost"] = source
    //context.log(sourcecategory,serviceBusTask.blobName,serviceBusTask.storageName,serviceBusTask.containerName);
    options.metadata["sourceCategory"] =  sourcecategory;
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
        urlString: getSumoEndpoint(serviceBusTask),
        MaxAttempts: 3,
        RetryInterval: 3000,
        compress_data: true,
        clientHeader: "blobreader-azure-function"
    };
    setSourceCategory(serviceBusTask, options, context);
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
                return releaseLockfromOffsetTable(ctx, serviceBusTask).then(function() {
                    return releaseMessagefromDLQ(ctx, serviceBusTask).then(function() {
                        ctx.done();
                    });
                });
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
            DLQMessage = lockedMessage;
            var serviceBusTask = JSON.parse(lockedMessage.body);
            if (serviceBusTask.blobType === "AppendBlob") {
                return releaseMessagefromDLQ(context, serviceBusTask).then(function() {
                    context.done();
                });
            }
            // Message received and locked and try to resend
            var options = {
                urlString: getSumoEndpoint(serviceBusTask),
                MaxAttempts: 3,
                RetryInterval: 3000,
                compress_data: true,
                clientHeader: "dlqblobreader-azure-function"
            };
            setSourceCategory(serviceBusTask, options, context);
            var sumoClient;
            function failureHandler(msgArray, ctx) {
                ctx.log("Failed to send to Sumo");
                if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                    ctx.done("DLQTaskConsumer failedmessages: " + sumoClient.messagesFailed);
                }
            }
            function successHandler(ctx) {
                ctx.log('Successfully sent to Sumo');
                if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                    if (sumoClient.messagesFailed > 0) {
                        ctx.done("DLQTaskConsumer failedmessages: " + sumoClient.messagesFailed);
                    } else {
                        ctx.log('Sent ' + sumoClient.messagesAttempted + ' data to Sumo. Exit now.');
                        return releaseLockfromOffsetTable(ctx, serviceBusTask).then(function() {
                            return releaseMessagefromDLQ(ctx, serviceBusTask).then(function() {
                                ctx.done();
                            });
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
                context.log("Error in reading messages from DLQ: ", error, typeof (error));
                context.done(error);
            }
        }

    });
}

module.exports = function (context, triggerData) {
    contentDownloaded = 0;
    context.log("Inside blob task consumer:", triggerData.rowKey);
    /*

    var triggerData = {
        subscriptionId: "c088dc46-d692-42ad-a4b6-9a542d28ad2a",
        resourceGroupName: "SumoAuditCollection",
        storageName: "allbloblogseastus",
        containerName: "appendblobexp1002-3-104857600",
        blobName: 'appendblobfileallbloblogseastus_1002_3_104857600_1.json',
        startByte: 0,
        blobType: "AppendBlob",
        rowKey: "allbloblogseastus-appendblobexp1002-3-104857600-appendblobfileallbloblogseastus_1002_3_104857600_1.json"
    };
    appendBlobStreamMessageHandlerv2(context, triggerData);
    // appendBlobStreamMessageHandler(context, triggerData);
    //servicebushandler(context, triggerData);
    */


    if (triggerData.isPastDue === undefined) {
        DLQMessage = null;
        // Todo: create two separate queues in the same namespace for appendblob and block blobs
        if (triggerData.blobType === undefined || triggerData.blobType === "BlockBlob") {
            servicebushandler(context, triggerData);
        } else {
            appendBlobStreamMessageHandlerv2(context, triggerData);
        }

    } else {
        // Todo: Test with old blockblob flow and remove appendblob from it's code may be separate out code in commonjs
        timetriggerhandler(context, triggerData);
    }

};