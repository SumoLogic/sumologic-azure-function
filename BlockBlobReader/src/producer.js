/////////////////////////////////////////////////////////////////////////////////////////////////
//           Function to create Block Blob tasks using EventGrid Events into Azure Service Bus //
/////////////////////////////////////////////////////////////////////////////////////////////////

var storage = require('azure-storage');
var tableService = storage.createTableService(process.env.APPSETTING_AzureWebJobsStorage);

function getRowKey(url) {
    var storageName = url.split("//").pop().split(".")[0];
    var arr = url.split('/').slice(3);
    var keyArr = [storageName];
    Array.prototype.push.apply(keyArr, arr);
    return keyArr.join("-");
}

function getBlobMetadata(message, contentLength) {
    var url = message.data.url;
    var data = url.split('/');
    var topicArr = message.topic.split('/');

    //topicArr: '/subscriptions/c088dc46-d692-42ad-a4b6-9a542d28ad2a/resourceGroups/AG-SUMO/providers/Microsoft.Storage/
    // url: 'https://allbloblogs.blob.core.windows.net/webapplogs/AZUREAUDITEVENTHUB/2018/04/26/09/f4f692.log'
    return {
        rowKey: getRowKey(url),
        containerName: data[3],
        blobName: data.slice(4).join('/'),
        storageName: url.split("//").pop().split(".")[0],
        resourceGroupName: topicArr[4],
        subscriptionId: topicArr[2],
        blobType: contentLength === 0 ? "AppendBlob" : "BlockBlob"
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
function getEntity(metadata, startByte, currentEtag) {
    //a single entity group transaction is limited to 100 entities. Also, the entire payload of the transaction may not exceed 4MB
    var entGen = storage.TableUtilities.entityGenerator;
    // RowKey/Partition key cannot contain "/"
    var entity = {
        PartitionKey: entGen.String(metadata.containerName),
        RowKey: entGen.String(metadata.rowKey),
        blobName: entGen.String(metadata.blobName),
        containerName: entGen.String(metadata.containerName),
        storageName: entGen.String(metadata.storageName),
        offset: entGen.Int64(startByte),
        eventdate: entGen.DateTime((new Date()).toISOString()),
        blobType: entGen.String(metadata.blobType),
        done: entGen.Boolean(false),
        resourceGroupName: entGen.String(metadata.resourceGroupName),
        subscriptionId: entGen.String(metadata.subscriptionId)
    };
    if (currentEtag) {
        entity['.metadata'] = { etag: currentEtag };
    }


    return entity;
}
/**
 * @param  {} eventHubMessages
 * @param  {} allcontentlengths
 * @param  {} metadatamap
 * @param  {} context
 * it creates a map {rowkey: [len1, len2, len3]}
 * This is done to take care of the ordering of events in a batch
 */
function getContentLengthPerBlob(eventHubMessages, allcontentlengths, metadatamap, context) {
    eventHubMessages.forEach(function (message) {
        var contentLength = message.data.contentLength;
        var metadata = getBlobMetadata(message, contentLength);
        var RowKey = getRowKey(message.data.url);
        metadatamap[RowKey] = metadata;
        if (contentLength >= 0) {
            (allcontentlengths[RowKey] || (allcontentlengths[RowKey] = [])).push(contentLength);
        }
    });
}
/**
 * @param  {} PartitionKey
 * @param  {} RowKey
 * @param  {} context
 *
 * retrieves the offset for a row from the table
 */
function getBlobPointerMap(PartitionKey, RowKey, context) {
    // Todo Add retries for node migration in cases of timeouts(non 400 & 500 errors)
    return new Promise(function (resolve, reject) {
        tableService.retrieveEntity(process.env.APPSETTING_TABLE_NAME, PartitionKey, RowKey, function (error, result, response) {
            // context.log("inside getBlobPointerMap", response.statusCode);
            if (response.statusCode === 404 || !error) {
                resolve(response);
            } else {
                reject(error);
            }
        });
    });
}

function updateBlobPointerMap(entity, context) {
    return new Promise(function (resolve, reject) {
        var insertOrReplace = ".metadata" in entity ? tableService.replaceEntity.bind(tableService) : tableService.insertEntity.bind(tableService);
        insertOrReplace(process.env.APPSETTING_TABLE_NAME, entity, function (error, result, response) {
            // context.log("inside updateBlobPointerMap", response.statusCode);
            if (!error) {
                resolve(response);
            } else {
                reject(error);
            }
        });
    });
}
/**
 * @param  {} PartitionKey
 * @param  {} RowKey
 * @param  {} sortedcontentlengths
 * @param  {} context
 * @param  {} metadata
 * @param  {} finalcontext
 *
 * If the contentLength is 0 then it is assumed to be Append Blobs else it is Block Blob
 * In case of Append blob a row entry is created with negative offset and all the rows with Append blob blob types are polled by append blob producer function
 *
 * In cases of Block blob a task is created in service bus and consumer function consumes the task by download the file using the offset in task metadata and then sending to sumo logic's http endpoint
 */
function createTasksForBlob(PartitionKey, RowKey, sortedcontentlengths, context, metadata, finalcontext) {
    // context.log("inside createTasksForBlob", PartitionKey, RowKey, sortedcontentlengths, metadata);
    getBlobPointerMap(PartitionKey, RowKey, context).then(function (response) {
        var tasks = [];
        var currentoffset = response.statusCode === 404 ? -1 : Number(response.body.offset);
        var currentEtag = response.statusCode === 404 ? null : response.body['odata.etag'];
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
        //Todo:  Bulk Row creation will reduce num of api calls.
        if (lastoffset > currentoffset) { // modify offset only when it's been changed
            context.log("Block blob scenario updating offset to: " + lastoffset + " from: " + currentoffset+ " RowKey: ", RowKey)
            var entity = getEntity(metadata, lastoffset, currentEtag);
            updateBlobPointerMap(entity, context).then(function (response) {
                context.bindings.tasks = context.bindings.tasks.concat(tasks);
                finalcontext(null, tasks.length + " Tasks added for RowKey: " + RowKey);
            }).catch(function (err) {
                //handle catch with retry when If-match fails else other err
                if (err.code === "UpdateConditionNotSatisfied") {
                    context.log("Need to Retry: " + RowKey, entity);
                }
                finalcontext(err, "Unable to Update offset for RowKey: " + RowKey);

            });
        } else if (currentoffset === -1 && lastoffset === -1) {
            context.log("Append blob scenario create just an entry RowKey: ", RowKey)
            var entity = getEntity(metadata, 0, currentEtag);
            updateBlobPointerMap(entity, context).then(function (response) {
                finalcontext(null, "AppendBlob Entry added for RowKey: " + RowKey);
            }).catch(function (err) {
                //handle catch with retry when If-match fails else other err
                if (err.code === "UpdateConditionNotSatisfied") {
                    context.log("Need to Retry: " + RowKey, entity);
                }
                finalcontext(err, "Unable to Update offset for RowKey: " + RowKey);
            });

        } else {
            finalcontext(null, "No tasks created for RowKey: " + RowKey);
        }

    }).catch(function (err) {
        // unable to retrieve offset
        finalcontext(err, "Unable to Retrieve offset for RowKey: " + RowKey);
    });

}

module.exports = function (context, eventHubMessages) {
    try {
        context.log("blobtaskproducer message received: ", eventHubMessages.length);
        var metadatamap = {};
        var allcontentlengths = {};
        getContentLengthPerBlob(eventHubMessages, allcontentlengths, metadatamap, context);
        var processed = 0;
        context.bindings.tasks = [];
        var totalRows = Object.keys(allcontentlengths).length;
        var errArr = [], RowKey;
        for (RowKey in allcontentlengths) {
            var sortedcontentlengths = allcontentlengths[RowKey].sort(); // ensuring increasing order of contentlengths
            var metadata = metadatamap[RowKey];
            var PartitionKey = metadata.containerName;
            createTasksForBlob(PartitionKey, RowKey, sortedcontentlengths, context, metadata, function (err, msg) {
                processed += 1;
                // context.log(RowKey, processed, err, msg);
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
