///////////////////////////////////////////////////////////////////////////////////
//           Function to create tasks using EventGrid Events into Azure EventHubs               //
///////////////////////////////////////////////////////////////////////////////////

var sumoutils = require('./sumoutils.js');
const { TableClient } = require("@azure/data-tables");
const tableClient = TableClient.fromConnectionString(process.env.AzureWebJobsStorage, process.env.TABLE_NAME);
const MaxAttempts = 3
const RetryInterval = 3000

function getRowKey(metadata) {
    var storageName = metadata.url.split("//").pop().split(".")[0];
    var arr = metadata.url.split('/').slice(3);
    var keyArr = [storageName];
    Array.prototype.push.apply(keyArr, arr);
    return keyArr.join("-");
}

function getBlobMetadata(message) {
    var url = message.data.url;
    var data = url.split('/');
    var topicArr = message.topic.split('/');

    // '/subscriptions/c088dc46-d692-42ad-a4b6-9a542d28ad2a/resourceGroups/AG-SUMO/providers/Microsoft.Storage/
    //'https://allbloblogs.blob.core.windows.net/webapplogs/AZUREAUDITEVENTHUB/2018/04/26/09/f4f692.log'
    return {
        url: url,
        containerName: data[3],
        blobName: data.slice(4).join('/'),
        storageName: url.split("//").pop().split(".")[0],
        resourceGroupName: topicArr[4],
        subscriptionId: topicArr[2],
        blobType: "AppendBlob"
    };
}

/*
*  FileOffsetMap table has following fields
*  PartitionKey - container name
*  RowKey - combination of storage, containername and blobname
*  blobName - blobpath
*  containerName
*  storageName
*  resourceGroupName
*  subscriptionId
*  offset - stores the location to the last send data
*  eventdate - blob creation event date (set by TaskProducer)
*
*  Below Fields are specific to Append Blob only
*
*  lastEnqueLockTime - last Append Blob task enque(to Service Bus) date  (set by AppendBlob TaskProducer)
*  senddate - last successful Append Blob data send (non empty data to Sumo) date (set by TaskConsumer)
*  updatedate - last Append Blob task process date by Task Consumer
*  done - denotes that Append Blob task is locked for new task creation
*  blobType - AppendBlob/BlockBlob
*
*
 */
function getEntity(metadata, endByte, currentEtag) {
    //a single entity group transaction is limited to 100 entities. Also, the entire payload of the transaction may not exceed 4MB
    // rowKey/partitionKey cannot contain "/"
    var entity = {
        partitionKey: metadata.containerName,
        rowKey: getRowKey(metadata),
        blobName: metadata.blobName,
        containerName: metadata.containerName,
        storageName: metadata.storageName,
        offset: { type: "Int64", value: String(endByte) },
        eventdate: new Date().toISOString(),
        blobType: metadata.blobType,
        done: false,
        resourceGroupName: metadata.resourceGroupName,
        subscriptionId: metadata.subscriptionId
    };
    if (currentEtag) {
        entity['options'] = {
            ifMatch: currentEtag, // Replace with the current ETag value of the entity
        };
    }

    return entity;
}
/**
 * @param  {} eventHubMessages
 * @param  {} allcontentlengths
 * @param  {} metadatamap
 * it creates a map {rowkey: [len1, len2, len3]}
 * This is done to take care of the ordering of events in a batch
 */
function getContentLengthPerBlob(eventHubMessages, allcontentlengths, metadatamap) {
    eventHubMessages.forEach(function (message) {
        var contentLength = message.data.contentLength;
        var metadata = getBlobMetadata(message);
        var rowKey = getRowKey(metadata);
        metadatamap[rowKey] = metadata;
        if (contentLength >= 0) {
            (allcontentlengths[rowKey] || (allcontentlengths[rowKey] = [])).push(message.data.contentLength);
        }
    });
}


/**
 * Update Blob Pointer Map.
 * 
 * @param {Object} entity - The entity object to update or create.
 * @returns {Promise<Object>} - A promise that resolves to the response from updating or creating the entity.
 */
async function updateOrCreateBlobPointerMap(entity) {
    let response;
    if (entity.options) {
        let options = entity.options;
        // Remove options from entity
        delete entity.options;
        // Update entity with specified options
        response = await tableClient.updateEntity(entity, "Replace", options);
    } else {
        response = await tableClient.createEntity(entity);
    }
    return response;
}


/**
 * @param  {} PartitionKey
 * @param  {} RowKey
 * @param  {} context
 * @param  {} metadata
 * @param  {} finalcontext
 *
 * If the contentLength is 0 then it is assumed to be Append Blobs else it is Block Blob
 * In case of Append blob a row entry is created with negative offset and all the rows with Append blob blob types are polled by append blob producer function
 *
 */
async function createTasksForAppendBlob(partitionKey, rowKey, context, metadata) {
    //context.log("inside createTasksForAppendBlob", partitionKey, rowKey, sortedcontentlengths, metadata);

    context.log.verbose("Creating an entry for RowKey: ", rowKey)
    try {
        var entity = getEntity(metadata, 0, null);
        var response = await updateOrCreateBlobPointerMap(entity); // this will always create
        return Promise.resolve({ status: "success", rowKey: rowKey, message: "AppendBlob Entry added for RowKey: " + rowKey });
    } catch (err) {

        if (err.statusCode === 409 && JSON.stringify(err).includes("EntityAlreadyExists")) {
            context.log("AppendBlob Entry exists for RowKey: " + rowKey);
            return Promise.resolve({ status: "success", rowKey: rowKey, message: "AppendBlob Entry exists for RowKey: " + rowKey });
        } else if ((err.statusCode === 404) && JSON.stringify(err).includes("TableNotFound")) {
            try {
                context.log(`Creating table in storage account: ${process.env.TABLE_NAME}`);
                await tableClient.createTable();
                await new Promise(resolve => setTimeout(resolve, 10000)); // 10 second wait for the table to be created
                var response = await updateOrCreateBlobPointerMap(entity); // this will always create
                return Promise.resolve({ status: "success", rowKey: rowKey, message: "AppendBlob Entry added for RowKey: " + rowKey });
            } catch(err) {
                return Promise.reject({ status: "failed", rowKey: rowKey, message: `Failed to create table for rowKey: ${rowKey} error: ${JSON.stringify(err)}`});
            }

        } else {
            return Promise.reject({ status: "failed", rowKey: rowKey, message: `Failed to create AppendBlob Entry for rowKey: ${rowKey} error: ${JSON.stringify(err)}` });
        }

    }

}


function getNewTask(currentoffset, sortedcontentlengths, metadata) {
    var tasks = [];
    var lastoffset = currentoffset;
    var i, endByte, task;
    for (i = 0; i < sortedcontentlengths.length; i += 1) {
        endByte = sortedcontentlengths[i] - 1;
        if (endByte > lastoffset) {
            // this will remove duplicate contentlengths
            // to specify a range encompassing the first 512 bytes of a blob use x-ms-range: bytes=0-511  contentLength = 512
            // saving in offset: 511 endByte
            task = Object.assign({
                startByte: lastoffset + 1,
                endByte: endByte
            }, metadata);
            tasks.push(task);
            lastoffset = endByte;
        }
    }
    return [tasks, lastoffset];
}

/**
 * Filter messages for AppendBlob.
 *
 * @param {Array<Object>} messages - An array of message objects to filter.
 * @returns {Array<Object>} - An array containing only the messages with AppendBlob type.
 */
function filterAppendBlob(messages) {
    // Use Array.prototype.filter to filter messages for AppendBlob
    return messages.filter(message => {
        // Return true if the message's blobType is 'AppendBlob'
        return message.data.blobType === 'AppendBlob';
    });
}

/**
 * Filter messages by file extension.
 *
 * @param {Object} context - The context object for logging or other operations.
 * @param {Array<Object>} messages - An array of message objects to filter.
 * @returns {Array<Object>} - An array containing only the messages with supported file extensions.
 */
function filterByFileExtension(context, messages) {
    // List of supported file extensions
    var supportedExtensions = ['log', 'csv', 'json', 'blob', 'txt'];

    // Use Array.prototype.filter to filter messages based on extension
    return messages.filter(message => {
        // Extract file extension from message subject
        let fileExtension = message.subject.split(".").pop();

        // If no extension found
        if (fileExtension == message.subject) {
            context.log.verbose("Found file with no extension, accepting appendblob file as log file")
        }

        // Return true if the message's extension is in the list of supported extensions
        return fileExtension == message.subject || supportedExtensions.includes(fileExtension);
    });
}


module.exports = async function (context, eventHubMessages) {
    context.log("Inside append blob file tracker");
    // eventHubMessages = [
    //     [
    //         {
    //             topic: '/subscriptions/c088dc46-d692-42ad-a4b6-9a542d28ad2a/resourceGroups/testsumosarg290524101050/providers/Microsoft.Storage/storageAccounts/testsa290524101050',
    //             subject: '/blobServices/default/containers/testcontainer-29-05-24-10-10-50/blobs/test.blob',
    //             eventType: 'Microsoft.Storage.BlobCreated',
    //             id: '05bad26c-b01e-0064-59cf-6a5397068de0',
    //             data: {
    //                 api: 'PutBlob',
    //                 requestId: '05bad26c-b01e-0064-59cf-6a5397000000',
    //                 eTag: '0x8DC38E65A421B29',
    //                 contentType: 'text/csv',
    //                 contentLength: 0,
    //                 blobType: 'AppendBlob',
    //                 url: 'https://testsa290524101050.blob.core.windows.net/testcontainer-29-05-24-10-10-50/test.blob',
    //                 sequencer: '00000000000000000000000000010F8A0000000000047e92',
    //                 storageDiagnostics: { batchId: 'd7656e84-7006-0036-00cf-6a2f7f000000' }
    //             },
    //             dataVersion: '',
    //             metadataVersion: '1',
    //             eventTime: new Date().toISOString(),
    //         }
    //     ]
    // ]


    try {
        eventHubMessages = [].concat.apply([], eventHubMessages);

        context.log.verbose("appendblobfiletracker message received: ", eventHubMessages.length);
        var appendBlobMessages = filterAppendBlob(eventHubMessages);
        context.log.verbose("appendBlob message filtered", eventHubMessages.length);
        var filterMessages = filterByFileExtension(context, appendBlobMessages);
        context.log.verbose("fileExtension message filtered: ", eventHubMessages.length);

        if (filterMessages.length > 0) {
            var metadatamap = {};
            var allcontentlengths = {};
            getContentLengthPerBlob(filterMessages, allcontentlengths, metadatamap);
            var totalRowsCreated = 0, totalExistingRows = 0;
            var allRowPromises = [];
            var totalRows = Object.keys(allcontentlengths).length;
            var errArr = [], rowKey;
            for (rowKey in allcontentlengths) {
                var metadata = metadatamap[rowKey];
                var partitionKey = metadata.containerName;
                allRowPromises.push(sumoutils.p_retryMax(createTasksForAppendBlob, MaxAttempts, RetryInterval, [partitionKey, rowKey, context, metadata], context).catch((err) => err));
            }
            // Fail the function if any one of the files fail to get inserted into FileOffsetMap

            await Promise.all(allRowPromises).then((responseValues) => {

                for (let response of responseValues) {
                    if (response.status === "failed") {
                        errArr.push(response.message);
                    } else if(response.status === "success" && response.message.includes("Entry exists")) {
                        totalExistingRows += 1;
                    } else {
                        totalRowsCreated += 1;
                    }
                }
            });

            var msg = `FileOffSetMap Rows Created: ${totalRowsCreated} Existing Rows: ${totalExistingRows} Failed: ${errArr.length}`;
            context.log(msg);
            if (errArr.length > 0) {
                context.log.error(`Failed for payload: allcontentlengths: ${JSON.stringify(allcontentlengths)} ErrorResponse: ${errArr.join('\n')}`);
                context.done(msg)
            } else {
                context.done();
            }

        }
        else {
            context.log(`eventHubMessages might not pertain to appendblob or files with supported extensions, Exit now!`);
            context.done();
        }
    } catch (error) {
        context.log.error(error)
        context.done(error);
    }
};
