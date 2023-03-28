///////////////////////////////////////////////////////////////////////////////////
//           Function to create tasks using EventGrid Events into Azure EventHubs               //
///////////////////////////////////////////////////////////////////////////////////

const { TableClient } = require("@azure/data-tables");
const tableClient = TableClient.fromConnectionString(process.env.APPSETTING_AzureWebJobsStorage,process.env.APPSETTING_TABLE_NAME);

function getRowKey(metadata) {
    var storageName =  metadata.url.split("//").pop().split(".")[0];
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
        offset: endByte,
        date: (new Date()).toISOString()
    };
    if (currentEtag) {
        entity['.metadata'] = { etag: currentEtag };
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
    const entity = await tableClient.getEntity(partitionKey, rowKey).catch(function (error){
        // context.log(error)
        statusCode = 404;
        return null
    });
    return {statusCode: statusCode, entity: entity};
}

async function updateBlobPointerMap(entity, context) {
    var insertOrReplace = ".metadata" in entity ? tableClient.updateEntity.bind(tableClient) : tableClient.createEntity.bind(tableClient);
    const response = await insertOrReplace(entity).catch(function (error){
        // context.log(error)
        return error;
    });
    return response;
}

function createTasksForBlob(partitionKey, rowKey, sortedcontentlengths, context, metadata, finalcontext) {
    // context.log("inside createTasksForBlob", partitionKey, rowKey, sortedcontentlengths, metadata);
    getBlobPointerMap(partitionKey, rowKey, context).then(function (response) {
        var tasks = [];
        var currentoffset = response.statusCode === 404 ? -1 : Number(response.entity.offset);
        var currentEtag = response.statusCode === 404 ? null : response.entity.etag;
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
        if (lastoffset > currentoffset) { // modify offset only when it's been changed
            var entity = getEntity(metadata, lastoffset, currentEtag);
            updateBlobPointerMap(entity, context).then(function (response) {
                context.bindings.tasks = context.bindings.tasks.concat(tasks);
                finalcontext(null, tasks.length + " Tasks added for rowKey: " + rowKey);
            }).catch(function (err) {
                //handle catch with retry when If-match fails else other err
                if (err.code === "UpdateConditionNotSatisfied" && error.statusCode === 412) {
                    context.log("Need to Retry: " + rowKey, entity);
                }
                finalcontext(err, "Unable to Update offset for rowKey: " + rowKey);

            });
        } else {
            finalcontext(null, "No tasks created for rowKey: " + rowKey);
        }

    }).catch(function (err) {
        // unable to retrieve offset
        finalcontext(err, "Unable to Retrieve offset for rowKey: " + rowKey);
    });

}

module.exports = function (context, eventHubMessages) {
    try {
        eventHubMessages = [].concat.apply([], eventHubMessages);
        // context.log("blobtaskproducer message received: ", eventHubMessages.length);
        var metadatamap = {};
        var allcontentlengths = {};
        getContentLengthPerBlob(eventHubMessages, allcontentlengths, metadatamap);
        var processed = 0;
        context.bindings.tasks = [];
        var totalRows = Object.keys(allcontentlengths).length;
        var errArr = [], rowKey;
        for (rowKey in allcontentlengths) {
            var sortedcontentlengths = allcontentlengths[rowKey].sort(); // ensuring increasing order of contentlengths
            var metadata = metadatamap[rowKey];
            var partitionKey = metadata.containerName;
            createTasksForBlob(partitionKey, rowKey, sortedcontentlengths, context, metadata, function (err, msg) {
                processed += 1;
                // context.log(rowKey, processed, err, msg);
                if (err) {
                    errArr.push(err);
                }
                if (totalRows === processed) {
                    context.log("Tasks Created: " + JSON.stringify(context.bindings.tasks) + " Blobpaths: " + JSON.stringify(allcontentlengths));
                    if (errArr.length > 0) {
                        context.done(errArr.join('\n'));
                    } else {
                        context.done();
                    }
                }
            });

        }
    } catch (error) {
        context.done(error);
    }
};
