//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Function to read from an Azure Storage Account by consuming task from Service Bus and send data to SumoLogic //
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var sumoHttp = require('./sumoclient');

const { ContainerClient, BlobServiceClient } = require("@azure/storage-blob");
const { DefaultAzureCredential } = require("@azure/identity");
const { AbortController } = require("@azure/abort-controller");
const { ServiceBusClient } = require("@azure/service-bus");
const { TableClient } = require("@azure/data-tables");
const azureTableClient = TableClient.fromConnectionString(process.env.APPSETTING_AzureWebJobsStorage, 'FileOffsetMap');
const blobServiceClient = BlobServiceClient.fromConnectionString(process.env.APPSETTING_AzureBlobStorage);

var DEFAULT_CSV_SEPARATOR = ",";
var MAX_CHUNK_SIZE = 1024;
var JSON_BLOB_HEAD_BYTES = 12;
var JSON_BLOB_TAIL_BYTES = 2;
var contentDownloaded = 0;

const tokenCredential = new DefaultAzureCredential();

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
function nsgLogsHandler(msg) {

    var jsonArray = [];
    msg = msg.trim().replace(/(^,)|(,$)/g, ""); //removing trailing spaces,newlines and leftover commas
    jsonArray = JSON.parse("[" + msg + "]");
    var eventsArr = [];
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
    msg = JSON.stringify(msg)
    msg = msg.trim().replace(/(^,)|(,$)/g, ""); //removing trailing spaces,newlines and leftover commas
    jsonArray = JSON.parse("[" + msg + "]");
    return jsonArray;
}
/**
 * @param  {} msg
 * Handler for json line format where every line is a json object
 */
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

async function getUpdatedEntity(task, endByte) {
    //a single entity group transaction is limited to 100 entities. Also, the entire payload of the transaction may not exceed 4MB
    // RowKey/Partition key cannot contain "/"
    // sets the offset updatedate done(releases the enque lock)

    let entity = await azureTableClient.getEntity(task.containerName, task.rowKey);
    entity.done = false;
    entity.updatedate = new Date().toISOString();
    entity.offset = endByte;

    if (contentDownloaded > 0) {
        entity.senddate = new Date().toISOString();
    }
    return entity;
}
/**
 * @param  {} task
 * @param  {} blobResult
 *
 * updates the offset in FileOffsetMap table for append blob file rows after the data has been sent to sumo
 */

async function updateAppendBlobPointerMap(entity) {

    let response = await azureTableClient.updateEntity(entity, "Replace")
    return response;
}

function setAppendBlobOffset(context, serviceBusTask, dataLenSent) {
    return new Promise(async (resolve, reject) => {
        // Todo: this should be atomic update if other request decreases offset it shouldn't allow
        var newOffset = parseInt(serviceBusTask.startByte, 10) + dataLenSent;
        context.log.verbose("Attempting to update offset row: %s to: %d from: %d", serviceBusTask.rowKey, newOffset, serviceBusTask.startByte);
        entity = await getUpdatedEntity(serviceBusTask, newOffset);
        var updateResult = await updateAppendBlobPointerMap(entity)
        context.log("updateResult: ", updateResult)
    });
}

/**
 * @param  {} task
 * @param  {} blobService
 * @param  {} context
 *
 * fetching ranged data from a file in storage account
 */
function getData(task, blobService) {
    // Todo support for chunk reading(if range is large)
    // valid offset status code 206 (Partial Content).
    // invalid offset status code 416 (Requested Range Not Satisfiable)
    //context.log("Inside get data function:");
    return new Promise(async function (resolve, reject) {
        try {
            var buffer = Buffer.alloc(task.endByte - task.startByte + 1);
            await blobService.downloadToBuffer(buffer, task.startByte, (task.endByte - task.startByte + 1), {
                abortSignal: AbortController.timeout(30 * 60 * 1000),
                blockSize: 4 * 1024 * 1024,
                concurrency: 1
            });
            resolve(buffer.toString());
        } catch (err) {
            reject(err);
        }
    })
};

function getBlockBlobService(context, task) {
    return new Promise(function (resolve, reject) {
        try {

            var containerClient = new ContainerClient(
                `https://${task.storageName}.blob.core.windows.net/${task.containerName}`,
                tokenCredential
            );
            var blockBlobClient = containerClient.getBlockBlobClient(task.blobName);
            resolve(blockBlobClient);
        } catch (err) {
            reject(err);
        }
    })
};

function releaseLockfromOffsetTable(context, serviceBusTask, dataLenSent) {
    var curdataLenSent = dataLenSent || contentDownloaded;
    if (serviceBusTask.blobType === "AppendBlob") {
        return setAppendBlobOffset(context, serviceBusTask, curdataLenSent).catch(function (error) {
            // not failing with error because log will automatically released by appendblob
            context.log.error(`Error - Failed to update OffsetMap table, error: ${JSON.stringify(error)},  serviceBusTask: ${JSON.stringify(serviceBusTask)}, data: ${JSON.stringify(curdataLenSent)}`)
        });
    } else {
        return new Promise(function (resolve, reject) { resolve(); });
    }
}

function sendToSumoBlocking(chunk, sendOptions, context, isText) {

    return new Promise(function (resolve, reject) {

        function failureHandler(msgArray, ctx) {
            reject();
        }
        function successHandler(ctx) {
            ctx.log.verbose('Sent ' + sumoClient.messagesAttempted + ' data to Sumo.');
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
    return match ? string.lastIndexOf(match.slice(-1)) : -1;
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
        } else if (file_ext === "json") {
            if (serviceBusTask.containerName === "insights-logs-kube-audit") {
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
    let lastIndex = regexLastIndexOf(data, logRegex, firstIdx + 1);

    // data.substring method extracts the characters in a string between "start" and "end", not including "end" itself.
    let prefix = data.substring(0, firstIdx);
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
    } catch (e) {
        context.log.verbose("last chunk not json parseable so ignoring", suffix, lastIndex, e);
    }
    // ideally ignoredprefixLen should always be 0. it will be dropped for existing files
    // for new files offset will always start from date
    var ignoredprefixLen = Buffer.byteLength(prefix, defaultEncoding);
    // data with both prefix and suffix removed
    data = data.substring(firstIdx, lastIndex);
    // can't use matchAll since it's available only after version > 12
    let startpos = 0;
    let maxChunkSize = 1 * 1024 * 1024; // 1 MB
    while ((match = logRegex.exec(data)) !== null) {

        if (match.index - startpos >= maxChunkSize) {
            dataChunks.push(data.substring(startpos, match.index));
            context.log.verbose("new chunk %d %d", startpos, match.index);
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
    return new Promise(function (resolve, reject) {

        let promiseChain = Promise.resolve();
        const makeNextPromise = (chunk) => () => {
            return sendToSumoBlocking(chunk, sendOptions, context, true).then(function () {
                if ((numChunksSent >= 10 && numChunksSent % 10 === 0) || numChunksSent <= 2) {
                    context.log("sent chunks: ", numChunksSent);
                }
                numChunksSent += 1;
                dataLenSent += Buffer.byteLength(chunk);
            });
        };
        for (var i = 0; i < dataChunks.length; i++) {
            promiseChain = promiseChain.then(makeNextPromise(dataChunks[i]));
        }
        return promiseChain.catch(function (err) {
            context.log.error(`Error in sendDataToSumoUsingSplitHandler blob: ${serviceBusTask.rowKey} prefix: ${ignoredprefixLen} Sent ${dataLenSent} bytes of data. numChunksSent ${numChunksSent}`)
            resolve(dataLenSent + ignoredprefixLen);
        }).then(function () {
            if (dataChunks.length === 0) {
                context.log(`No chunks to send ${serviceBusTask.rowKey}`);
            } else if (numChunksSent === dataChunks.length) {
                if (numChunksSent === dataChunks.length) {
                    context.log(`All chunks sucessfully sent to sumo blob ${serviceBusTask.rowKey} prefix: ${ignoredprefixLen} Sent ${dataLenSent} bytes of data. numChunksSent ${numChunksSent}`);
                }
            }
            resolve(dataLenSent + ignoredprefixLen);
        });

    });

}
/*
    First all the data(of length batchsize) is fetched and then sequentially sends the data by splitting into 1 MB chunks and removing splitted chunks in boundary
 */
function errorHandler(err, serviceBusTask, context) {
    let discardError = false;
    let errMsg = (err !== undefined ? err.toString() : "");
    if (typeof errMsg === 'string' && (errMsg.indexOf("MSIAppServiceTokenCredentials.parseTokenResponse") >= 0 || errMsg.indexOf("SyntaxError: Unexpected end of JSON input") >= 0)) {
        context.log.verbose("Error in appendBlobStreamMessageHandlerv2 MSI Toke Error ignored: blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        discardError = true;
    } else if (err !== undefined && err.statusCode === 416 && err.code === "InvalidRange") {
        // here in case of appendblob data may not exists after startByte
        context.log.verbose("offset is already at the end startbyte: %d of blob: %s Exit now!", serviceBusTask.startByte, serviceBusTask.rowKey);
        discardError = true;
    } else if (err !== undefined && (err.statusCode === 503 || err.statusCode === 500 || err.statusCode == 429)) {
        context.log.verbose("Error in appendBlobStreamMessageHandlerv2 Potential throttling scenario: blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        discardError = true;
    } else if (err !== undefined && (err.code === "ContainerNotFound" || err.code === "BlobNotFound" || err.statusCode == 404)) {
        // sometimes it may happen that the file/container gets deleted
        context.log.verbose("Error in appendBlobStreamMessageHandlerv2 File location doesn't exists:  blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        discardError = true;
    } else if (err !== undefined && (err.code === "ECONNREFUSED" || err.code === "ECONNRESET" || err.code === "ETIMEDOUT")) {
        context.log.verbose("Error in appendBlobStreamMessageHandlerv2 Connection Refused Error: blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        discardError = true;

    } else {
        context.log.error(`Error in appendBlobStreamMessageHandlerv2 Unknown error blob rowKey: ${serviceBusTask.rowKey}, startByte: ${serviceBusTask.startByte}, endByte: ${serviceBusTask.endByte}, statusCode: ${err.statusCode}, code: ${err.code} Exit now!`);
    }
    return discardError;
}

async function archiveIngestedFile(serviceBusTask, context) {
    await azureTableClient.deleteEntity(serviceBusTask.containerName, serviceBusTask.rowKey);
    context.done("Entity deleted")
}

function appendBlobStreamMessageHandlerv2(context, serviceBusTask) {

    var file_ext = serviceBusTask.blobName.split(".").pop();
    if (file_ext == serviceBusTask.blobName) {
        file_ext = "log";
    }
    var msghandler = { "log": logHandler, "csv": csvHandler, "json": jsonHandler, "blob": blobHandler, "nsg": nsgLogsHandler };
    if (!(file_ext in msghandler)) {
        context.log.error("Error in messageHandler: Unknown file extension - " + file_ext + " for blob: " + serviceBusTask.blobName);
        context.done();
        return;
    }

    var sendOptions = {
        urlString: process.env.APPSETTING_SumoLogEndpoint,
        MaxAttempts: 3,
        RetryInterval: 3000,
        compress_data: true,
        clientHeader: "blobreader-azure-function"
    };
    setSourceCategory(serviceBusTask, sendOptions);

    return getBlockBlobService(context, serviceBusTask).then(async function (blobService) {
        context.log("fetching blob %s %d %d", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte);
        var numChunks = 0;
        let batchSize = setAppendBlobBatchSize(serviceBusTask); // default batch size from sdk code
        context.log("setting batchsize", batchSize);
        var dataLen = 0;
        var dataChunks = [];
        let options = {
            "rangeStart": serviceBusTask.startByte,
            "rangeEnd": serviceBusTask.startByte + batchSize - 1,
            "skipSizeCheck": false, // if true it will call getblobProperties and makes decision to download full(returns _getBlobToStream) or download in range (returns _getBlobToRangeStream)
            // "speedSummary" // Todo: check whether it will print something
            // "parallelOperationThreadCount": 1 // default is 1 for appendblobToText and 5 for others
            // timeoutIntervalInMs server timeout
            // clientRequestTimeoutInMs client timeout
        };

        // let options = {
        /** Offset byte to start download from. */
        //     offset:
        /** Max content length in bytes. */
        //     length:
        // }

        // let containerClient = new ContainerClient(
        //     `https://${serviceBusTask.storageName}.blob.core.windows.net/${serviceBusTask.containerName}`,
        //     tokenCredential
        // );
        let containerClient = blobServiceClient.getContainerClient(serviceBusTask.containerName);
        let blockBlobClient = containerClient.getBlockBlobClient(serviceBusTask.blobName);

        // Download blob content
        context.log("// Download blob content...");
        let downloadBlockBlobResponse = await blockBlobClient.download();
        let readStream = downloadBlockBlobResponse.readableStreamBody
        context.log("Downloaded blob content");
        context.log(`requestId - ${downloadBlockBlobResponse.requestId}, statusCode - ${downloadBlockBlobResponse._response.status}`);

        readStream.on("data", (data) => {
            // Todo: returns 4 MB chunks we may need to break it to 1MB
            if ((numChunks >= 10 && numChunks % 10 === 0) || numChunks <= 2) {
                context.log(`Received ${data.length} bytes of data. numChunks ${numChunks}`);
            }
            dataLen += data.length;
            numChunks += 1;
            dataChunks.push(Buffer.isBuffer(data) ? data : Buffer.from(data));
        });

        readStream.on("end", function (res) {
            context.log(`Finished Fetching numChunks: ${numChunks} dataLen: ${dataLen} rowKey: ${serviceBusTask.rowKey}`);
            return sendDataToSumoUsingSplitHandler(context, Buffer.concat(dataChunks), sendOptions, serviceBusTask).then(function (dataLenSent) {
                contentDownloaded = dataLenSent;
                return releaseLockfromOffsetTable(context, serviceBusTask, dataLenSent).then(function () {
                    context.done();
                });
            }).catch(function (err) {
                context.log.error(`Error in sendDataToSumoUsingSplitHandler: ${serviceBusTask.rowKey} err ${err}`)
                context.done();
            });
        });

        readStream.on('error', function (err) {
            context.log("readStream error:" + JSON.stringify(err))
            // Todo: Test error cases for fetching
            return sendDataToSumoUsingSplitHandler(context, Buffer.concat(dataChunks), sendOptions, serviceBusTask).then(function (dataLenSent) {
                contentDownloaded = dataLenSent;
                let discardError = errorHandler(err, serviceBusTask, context);
                if (err !== undefined && (err.code === "BlobNotFound" || err.statusCode == 404)) {
                    // delete the entry from table storage
                    return archiveIngestedFile(serviceBusTask, context);
                } else {
                    return releaseLockfromOffsetTable(context, serviceBusTask, dataLenSent).then(function () {
                        if (discardError) {
                            context.done()
                        } else {
                            // after 1 hr lock automatically releases
                            context.done(err);
                        }
                    });
                }

            }).catch(function (err) {
                context.log.error(`Error in sendDataToSumoUsingSplitHandler inside OnError: ${serviceBusTask.rowKey} err ${err}`)
                return releaseLockfromOffsetTable(context, serviceBusTask).then(function () {
                    context.done();
                });
            });

        });

    }).catch(function (err) {
        let discardError = errorHandler(err, serviceBusTask, context);
        return releaseLockfromOffsetTable(context, serviceBusTask).then(function () {
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
    var file_ext = serviceBusTask.blobName.split(".").pop();
    if (file_ext == serviceBusTask.blobName) {
        file_ext = "log";
    }
    var msghandler = { "log": logHandler, "csv": csvHandler, "json": jsonHandler, "blob": blobHandler, "nsg": nsgLogsHandler };
    if (!(file_ext in msghandler)) {
        context.log.error("Error in messageHandler: Unknown file extension - " + file_ext + " for blob: " + serviceBusTask.blobName);
        context.done();
        return;
    }
    if (file_ext === "json" & serviceBusTask.containerName === "insights-logs-networksecuritygroupflowevent") {
        // because in json first block and last block remain as it is and azure service adds new block in 2nd last pos
        if (serviceBusTask.endByte < JSON_BLOB_HEAD_BYTES + JSON_BLOB_TAIL_BYTES) {
            context.done(); //rejecting first commit when no data is there data will always be atleast HEAD_BYTES+DATA_BYTES+TAIL_BYTES
            return;
        }
        serviceBusTask.endByte -= JSON_BLOB_TAIL_BYTES;
        if (serviceBusTask.startByte <= JSON_BLOB_HEAD_BYTES) {
            serviceBusTask.startByte = JSON_BLOB_HEAD_BYTES;
        } else {
            serviceBusTask.startByte -= 1; //to remove comma before json object
        }
        file_ext = "nsg";
    }
    getBlockBlobService(context, serviceBusTask).then(function (blobService) {
        return getData(serviceBusTask, blobService, context).then(function (msg) {
            context.log("Sucessfully downloaded blob %s %d %d", serviceBusTask.blobName, serviceBusTask.startByte, serviceBusTask.endByte);
            var messageArray;
            if (file_ext === "csv") {
                return getcsvHeader(serviceBusTask.containerName, serviceBusTask.blobName, blobService, context).then(function (headers) {
                    context.log("Received headers %d", headers.length);
                    messageArray = msghandler[file_ext](context, msg, headers);
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
                messageArray = msghandler[file_ext](context, msg);
                messageArray.forEach(function (msg) {
                    sumoClient.addData(msg);
                });
                sumoClient.flushAll();
            }
        });
    }).catch(function (err) {
        if (err.statusCode === 404) {
            context.log.error("Error in messageHandler: blob file doesn't exist " + serviceBusTask.blobName + " " + serviceBusTask.startByte + " " + serviceBusTask.endByte);
            context.done()
        } else {
            context.log.error("Error in messageHandler: Failed to send blob " + serviceBusTask.blobName + " " + serviceBusTask.startByte + " " + serviceBusTask.endByte);
            context.done(err);
        }

    });
}

function setSourceCategory(serviceBusTask, options) {

    options.metadata = options.metadata || {};
    options.metadata["name"] = serviceBusTask.storageName + "/" + serviceBusTask.containerName + "/" + serviceBusTask.blobName;
    // options.metadata["category"] = <custom source category>
}

function servicebushandler(context, serviceBusTask) {
    var sumoClient;

    var sendOptions = {
        urlString: process.env.APPSETTING_SumoLogEndpoint,
        MaxAttempts: 3,
        RetryInterval: 3000,
        compress_data: true,
        clientHeader: "blobreader-azure-function"
    };
    setSourceCategory(serviceBusTask, sendOptions);
    function failureHandler(msgArray, ctx) {
        ctx.log("ServiceBus Task: ", serviceBusTask)
        ctx.log.error("Failed to send to Sumo");
        if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
            ctx.done("TaskConsumer failedmessages: " + sumoClient.messagesFailed);
        }
    }
    function successHandler(ctx) {
        if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
            ctx.log("ServiceBus Task: ", serviceBusTask)
            if (sumoClient.messagesFailed > 0) {
                ctx.log.error('Failed to send few messages to Sumo')
                ctx.done("TaskConsumer failedmessages: " + sumoClient.messagesFailed);
            } else {
                ctx.log('Successfully sent to Sumo, Exiting now.');
                ctx.done();
            }
        }
    }

    sumoClient = new sumoHttp.SumoClient(sendOptions, context, failureHandler, successHandler);
    //context.log("Reached inside ServiceBus Handler")
    messageHandler(serviceBusTask, context, sumoClient);

}

async function timetriggerhandler(context, timetrigger) {

    if (timetrigger.isPastDue) {
        context.log("timetriggerhandler running late");
    }
    try {
        var sbClient = new ServiceBusClient(process.env.APPSETTING_TaskQueueConnectionString);
        var queueReceiver = sbClient.createReceiver(process.env.APPSETTING_TASKQUEUE_NAME, { subQueueType: "deadLetter", receiveMode: "peekLock" });
    } catch (err) {
        context.log.error("Failed to create service bus client and receiver");
        context.done(err);
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
        var sendOptions = {
            urlString: process.env.APPSETTING_SumoLogEndpoint,
            MaxAttempts: 3,
            RetryInterval: 3000,
            compress_data: true,
            clientHeader: "dlqblobreader-azure-function"
        };
        setSourceCategory(serviceBusTask, sendOptions);
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
                    try {
                        await queueReceiver.completeMessage(messages[0]);
                    } catch (err) {
                        await queueReceiver.close();
                        await sbClient.close();
                        if (!err) {
                            ctx.log("sent and deleted");
                            ctx.done();
                        } else {
                            ctx.log.verbose("Messages Sent but failed delete from DeadLetterQueue");
                            ctx.done(err);
                        }
                    }
                }
            }
        }
        sumoClient = new sumoHttp.SumoClient(sendOptions, context, failureHandler, successHandler);
        messageHandler(serviceBusTask, context, sumoClient);
    } catch (error) {
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
    contentDownloaded = 0;
    context.log("Inside blob task consumer:", triggerData.rowKey);

    /*
    triggerData = {
        subscriptionId: "c088dc46-d692-42ad-a4b6-9a542d28ad2a",
        resourceGroupName: "SumoAuditCollection",
        storageName: "allbloblogseastus",
        containerName: "testabb",
        blobName: 'blob_fixtures.log',
        startByte: 0,
        blobType: "AppendBlob",
        rowKey: "allbloblogseastus-testabb-blob_fixtures.log"
    };

    //appendBlobStreamMessageHandlerv2(context, triggerData);
    //appendBlobStreamMessageHandler(context, triggerData);
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