//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Function to read from an Azure Storage Account by consuming task from Service Bus and send data to SumoLogic //
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const { ContainerClient } = require("@azure/storage-blob");
const { DefaultAzureCredential } = require("@azure/identity");
const { TableClient } = require("@azure/data-tables");
const { decodeDataChunks } = require('./decodeDataChunks');
const { sendDataToSumoUsingSplitHandler } = require('./sendDataToSumoUsingSplitHandler');
const azureTableClient = TableClient.fromConnectionString(process.env.AzureWebJobsStorage, process.env.TABLE_NAME);
const MaxAttempts = 3

var DEFAULT_CSV_SEPARATOR = ",";
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
 * @param  {} context
 * @param  {} msg
 * @param  {} serviceBusTask
 * Handler for json line format where every line is a json object
 */
function jsonlineHandler(context, msg, serviceBusTask) {
    // it's assumed that .blob files contains json separated by \n
    //https://docs.microsoft.com/en-us/azure/application-insights/app-insights-export-telemetry

    let jsonArray = [];
    msg = msg.replace(/\0/g, '');
    msg = msg.replace(/}\r?\n{/g, "},{")
    msg = msg.trim().replace(/(^,+)|(,+$)/g, ""); //removing trailing spaces,newlines and leftover commas

    try {
        jsonArray = JSON.parse("[" + msg + "]");
    } catch (e) {
        context.log("JSON ParseException in blobHandler");
        context.log(e, msg);
        // removing unparsed prefix and suffix
        let start_idx = msg.indexOf('{');
        let last_idx = msg.lastIndexOf('}');
        let submsg = msg.substr(start_idx, last_idx + 1 - start_idx); // prefix & suffix removed
        try {
            jsonArray = JSON.parse("[" + submsg.replace(/}\r?\n{/g, "},{") + "]");
            let suffixlen = msg.length - (last_idx + 1);
            contentDownloaded -= suffixlen;
        } catch (e) {
            context.log("JSON ParseException in blobHandler for rowKey: " + serviceBusTask.rowKey + " with submsg ", start_idx, last_idx, msg.substr(0, start_idx), msg.substr(last_idx + 1));
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
    // RowKey/Partition key cannot contain "/"
    // sets the offset updatedate done(releases the enque lock)

    let entity = {
        done: false,
        updatedate: new Date().toISOString(),
        offset: { type: "Int64", value: String(endByte) },
        // In a scenario where the entity could have been deleted (archived) by appendblob because of large queueing time so to avoid error in insertOrMerge Entity we include rest of the fields like storageName,containerName etc.
        partitionKey: task.containerName,
        rowKey: task.rowKey,
        blobName: task.blobName,
        containerName: task.containerName,
        storageName: task.storageName,
        blobType: task.blobType,
        resourceGroupName: task.resourceGroupName,
        subscriptionId: task.subscriptionId
    }
    if (contentDownloaded > 0) {
        entity.senddate = new Date().toISOString()
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
    return await azureTableClient.updateEntity(entity, "Merge");
}

async function setAppendBlobOffset(context, serviceBusTask, newOffset) {
    return new Promise(async (resolve, reject) => {
        try {
            // Todo: this should be atomic update if other request decreases offset it shouldn't allow
            context.log.verbose("Attempting to update offset row: %s to: %d from: %d", serviceBusTask.rowKey, newOffset, serviceBusTask.startByte);
            var entity = getUpdatedEntity(serviceBusTask, newOffset)
            var updateResult = await updateAppendBlobPointerMap(entity)
            context.log("Update offset result: ", updateResult)
            resolve();
        } catch (error) {
            reject(error)
        }
    });
}

async function releaseLockfromOffsetTable(context, serviceBusTask, dataLenSent) {
    var curdataLenSent = dataLenSent || contentDownloaded;
    var newOffset = parseInt(serviceBusTask.startByte, 10) + curdataLenSent;
    return new Promise(async function (resolve, reject) {
        try {
            await setAppendBlobOffset(context, serviceBusTask, newOffset)
            if (serviceBusTask.startByte === newOffset) {
                context.log("Successfully updated Lock for Table row: " + serviceBusTask.rowKey + " Offset remains the same : " + newOffset);
            } else {
                context.log("Successfully updated OffsetMap, Table row: " + serviceBusTask.rowKey + ", To : " + newOffset + ", From: " + serviceBusTask.startByte);
            }
            resolve();
        } catch (error) {
            if (error !== undefined && (error.code === "ContainerNotFound" || error.code === "BlobNotFound" || error.statusCode == 404)) {
                context.log("Already Archived AppendBlob File with RowKey: " + serviceBusTask.rowKey);
                resolve();
            } else {
                context.log.error(`Error - Failed to update OffsetMap table, error: ${JSON.stringify(error)},  serviceBusTask: ${JSON.stringify(serviceBusTask)}, data: ${JSON.stringify(curdataLenSent)}`)
                reject(error);
            }
        }
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
    First all the data(of length batchsize) is fetched and then sequentially sends the data by splitting into 1 MB chunks and removing splitted chunks in boundary
 */
function errorHandler(err, serviceBusTask, context) {
    let discardError = false;
    let errMsg = (err !== undefined ? err.toString() : "");
    if (typeof errMsg === 'string' && (errMsg.indexOf("MSIAppServiceTokenCredentials.parseTokenResponse") >= 0 || errMsg.indexOf("SyntaxError: Unexpected end of JSON input") >= 0)) {
        context.log.error("Error in appendBlobStreamMessageHandlerv2 MSI Toke Error ignored: blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        discardError = true;
    } else if (err !== undefined && err.statusCode === 416 && err.code === "InvalidRange") {
        // here in case of appendblob data may not exists after startByte
        context.log(`Offset is already at the end, byte: ${serviceBusTask.startByte} of blob: ${serviceBusTask.rowKey}. Exit now!.`);
        discardError = true;
    } else if (err !== undefined && (err.statusCode === 503 || err.statusCode === 500 || err.statusCode == 429)) {
        context.log.error("Error in appendBlobStreamMessageHandlerv2 Potential throttling scenario: blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        discardError = true;
    } else if (err !== undefined && (err.code === "ContainerNotFound" || err.code === "BlobNotFound" || err.statusCode == 404)) {
        // sometimes it may happen that the file/container gets deleted
        context.log.error("Error in appendBlobStreamMessageHandlerv2 File location doesn't exists:  blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        discardError = true;
    } else if (err !== undefined && (err.code === "ECONNREFUSED" || err.code === "ECONNRESET" || err.code === "ETIMEDOUT")) {
        context.log.error("Error in appendBlobStreamMessageHandlerv2 Connection Refused Error: blob %s %d %d %d %s Exit now!", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte, err.statusCode, err.code);
        discardError = true;

    } else {
        context.log.error(`Error in appendBlobStreamMessageHandlerv2 Unknown error blob rowKey: ${serviceBusTask.rowKey}, startByte: ${serviceBusTask.startByte}, endByte: ${serviceBusTask.endByte}, statusCode: ${err.statusCode}, code: ${err.code} Exit now!`);
    }
    return discardError;
}

/**
 * Archive an ingested file by deleting its entity from Azure Table Storage.
 * 
 * @param {Object} serviceBusTask - Task object associated with the service bus message.
 * @param {Object} context - The context object for logging or other operations.
 */
async function archiveIngestedFile(serviceBusTask, context) {
    try {
        // Delete entity from Azure Table Storage
        await azureTableClient.deleteEntity(serviceBusTask.containerName, serviceBusTask.rowKey);
        context.done("Entity deleted");
    } catch (error) {
        context.log.error(`failed to archive Ingested File : ${error}`);
        context.done();
    }
}


/**
 * Stream data from a readable stream to a buffer.
 * 
 * @param {Object} context - The context object for logging or other operations.
 * @param {ReadableStream} readableStream - The readable stream containing the data to be streamed.
 * @param {Object} serviceBusTask - Task object associated with the service bus message.
 * @returns {Promise<Buffer>} - A promise that resolves to a buffer containing the streamed data.
 */
async function streamToBuffer(context, readableStream, serviceBusTask) {
    return new Promise((resolve, reject) => {
        var dataLen = 0;
        var chunks = [];

        // Event listener for data chunks
        readableStream.on("data", (data) => {
            if ((chunks.length >= 10 && chunks.length % 10 === 0) || chunks.length <= 2) {
                context.log.verbose(`Received ${data.length} bytes of data. numChunks ${chunks.length}`);
            }
            // Accumulate data length and push data chunk to chunks array
            dataLen += data.length;
            chunks.push(Buffer.isBuffer(data) ? data : Buffer.from(data));
        });

        // Event listener for end of stream
        readableStream.on("end", () => {
            context.log(`Finished Fetching numChunks: ${chunks.length} dataLen: ${dataLen} rowKey: ${serviceBusTask.rowKey}`);
            // Resolve the promise with concatenated buffer of all chunks
            resolve(Buffer.concat(chunks));
        });

        // Event listener for stream errors
        readableStream.on('error', (err) => {
            // Reject the promise with the error if any
            reject(err);
        });
    });
}


/**
 * Task Handler method to collect and parse data from Append Blob, send to Sumo Collector/Source
 * 
 * @param {Object} serviceBusTask - The serviceBusTask object.
 * @param {Object} context - The context object for logging or other operations.
 */
async function appendBlobStreamMessageHandlerv2(context, serviceBusTask) {

    var file_ext = serviceBusTask.blobName.split(".").pop();
    if (file_ext == serviceBusTask.blobName) {
        file_ext = "log";
    }
    var msghandler = { "log": logHandler, "csv": csvHandler, "json": jsonlineHandler, "blob": jsonlineHandler };
    if (!(file_ext in msghandler)) {
        context.log.error("Error in messageHandler: Unknown file extension - " + file_ext + " for blob: " + serviceBusTask.blobName);
        context.done();
        return;
    }

    var sendOptions = {
        urlString: getSumoEndpoint(serviceBusTask),
        MaxAttempts: MaxAttempts,
        RetryInterval: 3000,
        compress_data: true,
        clientHeader: "appendblobreader-azure-function"
    };
    setSourceCategory(serviceBusTask, sendOptions);

    context.log.verbose("Fetching blob %s %d %d", serviceBusTask.rowKey, serviceBusTask.startByte, serviceBusTask.endByte);
    let batchSize = setAppendBlobBatchSize(serviceBusTask); // default batch size from sdk code
    context.log.verbose("Setting batch-size", batchSize);


    let options = {
        maxRetryRequests: MaxAttempts
    };

    try {

        var containerClient = new ContainerClient(
            `https://${serviceBusTask.storageName}.blob.core.windows.net/${serviceBusTask.containerName}`,
            tokenCredential
        );
        var blockBlobClient = containerClient.getBlockBlobClient(serviceBusTask.blobName);

        context.log.verbose(`Download blob content, offset: ${serviceBusTask.startByte}, count: ${serviceBusTask.startByte + batchSize - 1}, option: ${JSON.stringify(options)}`);
        let downloadBlockBlobResponse = await blockBlobClient.download(serviceBusTask.startByte, serviceBusTask.startByte + batchSize - 1, options);
        await streamToBuffer(context, downloadBlockBlobResponse.readableStreamBody, serviceBusTask).then(
            async function (value) {
                context.log.verbose("Successfully downloaded data, sending to SUMO.");
                await sendDataToSumoUsingSplitHandler(context, value, sendOptions, serviceBusTask).then(async (dataLenSent) => {
                    contentDownloaded = dataLenSent;
                    await releaseLockfromOffsetTable(context, serviceBusTask, dataLenSent).then(function () {
                        context.done();
                    });
                }).catch(function (err) {
                    context.log.error(`Error in sendDataToSumoUsingSplitHandler: ${serviceBusTask.rowKey} err ${err}`);
                    context.done();
                });
            },
            async function (error) {
                let discardError = errorHandler(error, serviceBusTask, context);
                if (error !== undefined && (error.code === "BlobNotFound" || error.statusCode == 404)) {
                    // delete the entry from table storage
                    archiveIngestedFile(serviceBusTask, context);
                } else {
                    await releaseLockfromOffsetTable(context, serviceBusTask, dataLenSent).then(function () {
                        if (discardError) {
                            context.done();
                        } else {
                            context.log.error("Failed to download data in streamToBuffer");

                            // after 1 hr lock automatically releases
                            context.done(err);
                        }
                    });
                }
            }
        );
        context.log(`RequestId - ${downloadBlockBlobResponse.requestId}, statusCode - ${downloadBlockBlobResponse._response.status}`);

    } catch (error) {
        let discardError = errorHandler(error, serviceBusTask, context);
        return await releaseLockfromOffsetTable(context, serviceBusTask).then(function () {
            if (discardError) {
                context.done();
            } else {
                context.log.error("Error while downloading and sending to sumo", error);
                // after 1 hr lock automatically releases
                context.done(error);
            }
        });
    }
}

/**
 * Truncates the given string if it exceeds 128 characters and creates a new string.
 * If the string is within the limit, returns the original string.
 * @param {string} data - The original data.
 * @returns {string} - The new string, truncated if necessary.
 */
function checkAndTruncate(data) {
    const maxLength = 128;

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
    options.metadata["sourceHost"] = `${serviceBusTask.storageName}/${serviceBusTask.containerName}`
    // context.log(serviceBusTask.blobName, serviceBusTask.storageName,serviceBusTask.containerName);
    // options.metadata["sourceCategory"] = "custom_source_category";
    options.metadata["sourceName"] = checkAndTruncate(serviceBusTask.blobName);
}

module.exports = async function run(context, triggerData) {
    contentDownloaded = 0;

    // triggerData = {
    //     "partitionKey": 'testcontainer-01-03-24-00-00-01',
    //     "rowKey": 'testsa010324000001-testcontainer-01-03-24-00-00-01-testblob',
    //     "containerName": 'testcontainer-01-03-24-00-00-01',
    //     "blobName": 'testblob',
    //     "storageName": 'testsa010324000001',
    //     "resourceGroupName": 'tabrsrg010324000001',
    //     "subscriptionId": 'c088dc46-d692-42ad-a4b6-9a542d28ad2a',
    //     "blobType": 'AppendBlob',
    //     "startByte": 0,
    //     "batchSize": 104857600
    // }
    
    context.log("Inside blob task consumer:", triggerData.rowKey);

    if (triggerData.blobType == 'AppendBlob'){
        await appendBlobStreamMessageHandlerv2(context, triggerData);
    }
    else{
        context.log(`triggerData blobType is ${triggerData.blobType}, Exit now!`);
        context.done()
    }
};
