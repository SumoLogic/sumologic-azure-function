///////////////////////////////////////////////////////////////////////////////////
//           Function to create tasks using EventGrid Events into Azure EventHubs               //
///////////////////////////////////////////////////////////////////////////////////

var storage = require('azure-storage');
var tableService = storage.createTableService(process.env.APPSETTING_AzureWebJobsStorage);

function getRowKey(metadata) {
    var storageName =  metadata.url.split("//").pop().split(".")[0]
    var arr = metadata.url.split('/').slice(3)
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
    var entGen = storage.TableUtilities.entityGenerator;
    // RowKey/Partition key cannot contain "/"
    var entity = {
        PartitionKey: entGen.String(metadata.containerName),
        RowKey: entGen.String(getRowKey(metadata)),
        blobName: entGen.String(metadata.blobName),
        containerName: entGen.String(metadata.containerName),
        storageName: entGen.String(metadata.storageName),
        offset: entGen.Int64(endByte),
        date: entGen.DateTime((new Date()).toISOString()),
    };
    if (currentEtag) {
        entity['.metadata'] = { etag: currentEtag };
    }


    return entity;
}

function getContentLengthPerBlob(eventHubMessages, allcontentlengths, metadatamap) {
    eventHubMessages.forEach(function (message) {
        var metadata = getBlobMetadata(message);
        var RowKey = getRowKey(metadata);
        metadatamap[RowKey] = metadata;
        (allcontentlengths[RowKey] || (allcontentlengths[RowKey] = [])).push(message.data.contentLength);
    });
}

function getBlobPointerMap(PartitionKey, RowKey, context) {
    // Todo Add retries for node migration in cases of timeouts(non 400 & 500 errors)
    return new Promise(function (resolve, reject) {
        tableService.retrieveEntity(process.env.APPSETTING_TABLE_NAME, PartitionKey, RowKey, function(error, result, response){
          // context.log("inside getBlobPointerMap", response.statusCode);
          if (response.statusCode == 404 || !error) {
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
        insertOrReplace(process.env.APPSETTING_TABLE_NAME, entity, function(error, result, response){
            // context.log("inside updateBlobPointerMap", response.statusCode);
            if(!error){
                resolve(response);
            } else {
                reject(error);
            }
        });
    });
}

function createTasksForBlob(PartitionKey, RowKey, sortedcontentlengths, context, metadata, finalcontext) {
    // context.log("inside createTasksForBlob", PartitionKey, RowKey, sortedcontentlengths, metadata);
    getBlobPointerMap(PartitionKey, RowKey, context).then(function (response) {
        var tasks = [];
        var currentoffset = response.statusCode == 404 ? -1 : Number(response.body.offset);
        var currentEtag = response.statusCode == 404 ? null : response.body['odata.etag']
        var lastoffset = currentoffset;
        for (var i = 0; i < sortedcontentlengths.length; i++) {
            var endByte = sortedcontentlengths[i]-1;
            if (endByte > lastoffset) {
                // this will remove duplicate contentlengths
                // to specify a range encompassing the first 512 bytes of a blob use x-ms-range: bytes=0-511  contentLength = 512
                // saving in offset: 511 endByte
                var task = Object.assign({
                    startByte: lastoffset+1,
                    endByte: endByte
                }, metadata);
                tasks.push(task);
                lastoffset = endByte;
            }
        }
        if (lastoffset > currentoffset) { // modify offset only when it's been changed
            var entity =  getEntity(metadata, lastoffset, currentEtag);
            updateBlobPointerMap(entity, context).then(function (response) {
                context.bindings.tasks = context.bindings.tasks.concat(tasks);
                finalcontext(null, tasks.length + " Tasks added for RowKey: " + RowKey);
            }).catch(function (err) {
                //handle catch with retry when If-match fails else other err
                if (err.code == "UpdateConditionNotSatisfied" && error.statusCode == 412) {
                    context.log("Need to Retry: " + RowKey, entity);
                }
                finalcontext(err, "Unable to Update offset for RowKey: " + RowKey);

            });
        } else {
            finalcontext(null,"No tasks created for RowKey: " + RowKey);
        }

    }).catch(function(err) {
        // unable to retrieve offset
        finalcontext(err, "Unable to Retrieve offset for RowKey: " + RowKey);
    });

}

module.exports = function (context, eventHubMessages) {
    // eventHubMessages = [
    //     {
    //     topic: '/subscriptions/c088dc46-d692-42ad-a4b6-9a542d28ad2a/resourceGroups/AG-SUMO/providers/Microsoft.Storage/storageAccounts/allbloblogs',
    //     subject: '/blobServices/default/containers/testcontainer/blobs/testblob.log',
    //     eventType: 'Microsoft.Storage.BlobCreated',
    //     id: '3d8882c6-301e-00c5-4ed8-f1d6f006fc8e',
    //     data:
    //         {
    //             api: 'PutBlockList',
    //             clientRequestId: '4c9605d1-5dcb-11e8-865d-dca904938b01',
    //             requestId: '3d8882c6-301e-00c5-4ed8-f1d6f0000000',
    //             eTag: '0x8D5BFEF31665795',
    //             contentType: 'application/octet-stream',
    //             contentLength: 60858,
    //             blobType: 'BlockBlob',
    //             url: 'https://allbloblogs.blob.core.windows.net/testcontainer/testblob.log',
    //             sequencer: '000000000000000000000000000008AA00000000011cc77e',
    //             storageDiagnostics: [Object]
    //         },
    //     dataVersion: '',
    //     metadataVersion: '1'
    //     }
    // ];
    try {
        // context.log("blobtaskproducer message received: ", eventHubMessages.length);
        var metadatamap = {};
        var allcontentlengths = {};
        getContentLengthPerBlob(eventHubMessages, allcontentlengths, metadatamap);
        var processed = 0;
        context.bindings.tasks = [];
        var totalRows = Object.keys(allcontentlengths).length;
        var errArr = [];
        for (var RowKey in allcontentlengths) {
            var sortedcontentlengths = allcontentlengths[RowKey].sort(); // ensuring increasing order of contentlengths
            var metadata = metadatamap[RowKey]
            var PartitionKey = metadata.containerName;
            createTasksForBlob(PartitionKey, RowKey, sortedcontentlengths, context, metadata, function(err, msg) {
                processed += 1;
                // context.log(RowKey, processed, err, msg);
                if (err) {
                    errArr.push(err);
                }
                if (totalRows == processed) {
                    context.log("Tasks Created: " + JSON.stringify(context.bindings.tasks) + " Blobpaths: " + JSON.stringify(allcontentlengths));
                    if (errArr.length > 0) {
                        context.done(errArr.join('\n'));
                    }
                    else {
                        context.done();
                    }
                }
            })

        }
    } catch(error) {
        context.done(error);
    }
};
