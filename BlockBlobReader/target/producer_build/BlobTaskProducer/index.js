///////////////////////////////////////////////////////////////////////////////////
//           Function to create tasks using EventGrid Events into Azure EventHubs               //
///////////////////////////////////////////////////////////////////////////////////

var sumoutils = require('./sumoutils.js');
var { TableClient } = require("@azure/data-tables");
var tableClient = TableClient.fromConnectionString(process.env.APPSETTING_AzureWebJobsStorage,process.env.APPSETTING_TABLE_NAME);
const MaxAttempts = 3
const RetryInterval = 3000

function getRowKey(metadata) {
    var storageName =  metadata.url.split("//").pop().split(".")[0];
    var arr = metadata.url.split('/').slice(3);
    var keyArr = [storageName];
    Array.prototype.push.apply(keyArr, arr);
    // key cannot be greater than 1KB or 1024 bytes;
    var rowKey = keyArr.join("-");
    return rowKey.substr(0,Math.min(1024, rowKey.length)).replace(/^-+|-+$/g, '');
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
        subscriptionId: topicArr[2]
    };
}

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
        date: (new Date()).toISOString()
    };
    if (currentEtag) {
        entity['options'] = {
            ifMatch: currentEtag, // Replace with the current ETag value of the entity
          };
    }
    return entity;
}

function getContentLengthPerBlob(eventHubMessages, allcontentlengths, metadatamap) {
    eventHubMessages.forEach(function (message) {
        var metadata = getBlobMetadata(message);
        var rowKey = getRowKey(metadata);
        metadatamap[rowKey] = metadata;
        (allcontentlengths[rowKey] || (allcontentlengths[rowKey] = [])).push(message.data.contentLength);
    });
}

async function getBlobPointerMap(partitionKey, rowKey, context) {
    // Todo Add retries for node migration in cases of timeouts(non 400 & 500 errors)
    var statusCode = 200;
    var entity;
    try {
        entity = await tableClient.getEntity(partitionKey, rowKey);
        //context.log("retreived existing rowkey: " + rowKey)
    } catch(err) {

        if ((err.statusCode === 404) && JSON.stringify(err).includes("TableNotFound")) {
            try {
                context.log(`Creating table in storage account: ${process.env.TABLE_NAME}`);
                await tableClient.createTable();
                await new Promise(resolve => setTimeout(resolve, 10000)); // 10 second wait for the table to be created
            } catch(err) {
                context.log(`Failed to create table for rowKey: ${rowKey} error: ${JSON.stringify(err)}`);
            }
        }
        // err object keys : [ 'name', 'code', 'statusCode', 'request', 'response', 'details' ]
        if(err.statusCode === 404) {
            //context.log("no existing row found, new file scenario for rowkey: " + rowKey)
            statusCode = 404;
        } else {
            throw err;
        }
    }
    //context.log({statusCode: statusCode, entity: entity});
    return {statusCode: statusCode, entity: entity};
}

async function updateBlobPointerMap(entity, context) {
    let response;
    if(entity.options){
        let options = entity.options;
        delete entity.options;
        response = await tableClient.updateEntity(entity,"Replace",options)
    }else{
        response = await tableClient.createEntity(entity)
    }
    return response;
}

function getNewTask(currentoffset,sortedcontentlengths,metadata){
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
    if (tasks.length === 0 && sortedcontentlengths.length > 0  && endByte < currentoffset) {
        // in NSG Flow logs sometimes file gets rewritten, hence starting the file from the beginning
        task = Object.assign({
            startByte: 0,
            endByte: endByte
        }, metadata);
        tasks.push(task);
        lastoffset = endByte;
    }
    return [tasks,lastoffset];
}

async function createTasksForBlob(partitionKey, rowKey, sortedcontentlengths, context, metadata) {
    //context.log("inside createTasksForBlob", partitionKey, rowKey, sortedcontentlengths, metadata);
    if (sortedcontentlengths.length === 0) {
        return Promise.resolve({status: "success", message: "No tasks created for rowKey: " + rowKey});
    }
    try{
        var retrievedResponse = await getBlobPointerMap(partitionKey, rowKey, context);
        //context.log("retrieved blob pointer successsfully for rowkey: " + rowKey + " response: "+ retrievedResponse)
    }catch(err){
       // unable to retrieve offset, hence ingesting whole file from starting byte
       let lastoffset = sortedcontentlengths[sortedcontentlengths.length - 1] - 1;
       return Promise.reject({status: "failed", rowKey: rowKey, message: "Unable to Retrieve offset for rowKey: " + rowKey + " Error: " + err, lastoffset : lastoffset, currentoffset: -1});
    }
    var currentoffset = retrievedResponse.statusCode === 404 ? -1 : Number(retrievedResponse.entity.offset);
    var currentEtag = retrievedResponse.statusCode === 404 ? null : retrievedResponse.entity.etag;
    var [tasks,lastoffset] = getNewTask(currentoffset, sortedcontentlengths, metadata);

    if (tasks.length > 0) { // modify offset only when it's been changed
        var entity = getEntity(metadata, lastoffset, currentEtag);
        try{
            var updatedResponse = await updateBlobPointerMap(entity, context);
            //context.log("updated blob pointer successsfully for rowkey: " + rowKey + " response: "+ updatedResponse)
            context.bindings.tasks = context.bindings.tasks.concat(tasks);
            return Promise.resolve({status: "success",rowKey: rowKey, message: tasks.length + " Tasks added for rowKey: " + rowKey});
        }catch(err){
            if (err && err.details && err.details.odataError && err.details.odataError.code === "UpdateConditionNotSatisfied" && err.statusCode === 412) {
                context.log.verbose("Need to Retry: " + rowKey);
            }
            return Promise.reject({status: "failed",rowKey: rowKey, message: "Unable to Update offset for rowKey: " + rowKey + " Error: " + err, lastoffset : lastoffset, currentoffset: currentoffset});
        }
    } else {
        return Promise.resolve({status: "success",rowKey: rowKey, message: "No tasks created for rowKey: " + rowKey});
    }
}

/**
 * Filter messages for BlockBlob.
 *
 * @param {Array<Object>} messages - An array of message objects to filter.
 * @returns {Array<Object>} - An array containing only the messages with BlockBlob type.
 */
function filterBlockBlob(messages) {
    // Use Array.prototype.filter to filter messages for BlockBlob
    return messages.filter(message => {
        // Return true if the message's blobType is 'BlockBlob'
        return message.data.blobType === 'BlockBlob';
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
            context.log.verbose("Found file with no extension, accepting blockblob file as log file")
        }

        // Return true if the message's extension is in the list of supported extensions
        return fileExtension == message.subject || supportedExtensions.includes(fileExtension);
    });
}

module.exports = async function (context, eventHubMessages) {
    try {
        eventHubMessages = [].concat.apply([], eventHubMessages);
        context.log.verbose("blockblobfiletracker message received: ", eventHubMessages.length);
        var blockBlobMessages = filterBlockBlob(eventHubMessages);
        context.log.verbose("blockBlob message filtered", eventHubMessages.length);
        var filterMessages = filterByFileExtension(context, blockBlobMessages);
        context.log.verbose("fileExtension message filtered: ", eventHubMessages.length);

        if (filterMessages.length > 0) {
            var metadatamap = {};
            var allcontentlengths = {};
            getContentLengthPerBlob(filterMessages, allcontentlengths, metadatamap);
            var processed = 0;
            context.bindings.tasks = [];
            var allRowPromises = [];
            var totalRows = Object.keys(allcontentlengths).length;
            var errArr = [], rowKey;
            for (rowKey in allcontentlengths) {
                var sortedcontentlengths = allcontentlengths[rowKey].sort(); // ensuring increasing order of contentlengths
                var metadata = metadatamap[rowKey];
                var partitionKey = metadata.containerName;
                allRowPromises.push(sumoutils.p_retryMax(createTasksForBlob,MaxAttempts,RetryInterval,[partitionKey, rowKey, sortedcontentlengths, context, metadata], context).catch((err) => err));
            }
            await Promise.all(allRowPromises).then((responseValues) => {
                    //creating duplicate task for file causing an error when update condition is not satisfied in mutiple read and write scenarios for same row key in fileOffSetMap table
                    for (let response of responseValues){
                        processed += 1;
                        if(response.status === "failed"){
                            context.log.verbose("creating duplicate task since retry failed for rowkey: " + response.rowKey);
                            var duplicateTask = Object.assign({
                                startByte: response.currentoffset + 1,
                                endByte: response.lastoffset
                            }, metadatamap[response.rowKey]);
                            context.bindings.tasks = context.bindings.tasks.concat([duplicateTask]);
                            errArr.push(response.message);
                        }
                    }
            });
            if (totalRows === processed) {
                context.log("Tasks Created: " + JSON.stringify(context.bindings.tasks) + " Blobpaths: " + JSON.stringify(allcontentlengths));
                if (errArr.length > 0) {
                    context.log.error(errArr.join('\n'));
                }
                context.done();
            }
        } else {
            context.log(`eventHubMessages might not pertain to blockblob or files with supported extensions, Exit now!`);
            context.done();
        }
    } catch (error) {
        context.log.error(error)
        context.done(error);
    }
};
