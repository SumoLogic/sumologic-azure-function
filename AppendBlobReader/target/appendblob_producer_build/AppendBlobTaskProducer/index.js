//////////////////////////////////////////////////////////////////////////////////////////////////////
//           Function to create Append Blob tasks using File OffsetMap Table into Azure Service Bus //
//////////////////////////////////////////////////////////////////////////////////////////////////////

var storageOld = require('azure-storage');
var tableService = storageOld.createTableService(process.env.APPSETTING_AzureWebJobsStorage);

var { TableClient } = require("@azure/data-tables");
var tableClient = TableClient.fromConnectionString(process.env.APPSETTING_AzureWebJobsStorage, process.env.APPSETTING_TABLE_NAME);

function queryEntitiesSegmented (table, tableQuery, continuationToken)  {
    return new Promise(function (resolve, reject)  {
        tableService.queryEntities(table, tableQuery, continuationToken, function (error, results) {
            if (error) { reject(error); }
            else { resolve(results); }
        });
    });
}

function getTask(entity) {
    return {
        rowKey: entity.RowKey._,
        containerName: entity.containerName._,
        blobName: entity.blobName._,
        storageName: entity.storageName._,
        resourceGroupName: entity.resourceGroupName._,
        subscriptionId: entity.subscriptionId._,
        blobType: "AppendBlob",
        startByte: parseInt(entity.offset._) ,
    };
}

function getLockedEntity(entity) {
    //a single entity group transaction is limited to 100 entities. Also, the entire payload of the transaction may not exceed 4MB
    var entGen = storage.TableUtilities.entityGenerator;
    // RowKey/Partition key cannot contain "/"
    // lastEnqueLockTime - it denotes the last time when it was enqueued in Service Bus
    // done - it is set to true which means the task is enqueued in Service Bus
    var entity = {
        PartitionKey: entity.PartitionKey,
        RowKey: entity.RowKey,
        done: entGen.Boolean(true),
        lastEnqueLockTime: entGen.DateTime((new Date()).toISOString()),
        // In a scenario where the entity could have been deleted (archived) by appendblob because of large queueing time so to avoid error in insertOrMerge Entity we include rest of the fields like storageName,containerName etc.
        blobName: entity.blobName,
        containerName: entity.containerName,
        storageName: entity.storageName,
        offset: entity.offset,
        blobType: entity.blobType,
        resourceGroupName: entity.resourceGroupName,
        subscriptionId: entity.subscriptionId
    };
    return entity;
}

function getunLockedEntity(entity) {
    //a single entity group transaction is limited to 100 entities. Also, the entire payload of the transaction may not exceed 4MB
    var entGen = storage.TableUtilities.entityGenerator;
    // RowKey/Partition key cannot contain "/"
    // lastEnqueLockTime - it denotes the last time when it was enqueued in Service Bus
    // done - it is set to true which means the task is enqueued in Service Bus
    var lastEnqueLockTime;
    if (entity.lastEnqueLockTime === undefined) {
        lastEnqueLockTime =  entity.eventdate;
    } else {
        lastEnqueLockTime = entity.lastEnqueLockTime;
    }
    var entity = {
        PartitionKey: entity.PartitionKey,
        RowKey: entity.RowKey,
        done: entGen.Boolean(false),
        // In a scenario where the entity could have been deleted (archived) by appendblob because of large queueing time so to avoid error in insertOrMerge Entity we include rest of the fields like storageName,containerName etc.
        lastEnqueLockTime: lastEnqueLockTime,
        eventdate: entity.eventdate,
        blobName: entity.blobName,
        containerName: entity.containerName,
        storageName: entity.storageName,
        offset: entity.offset,
        blobType: entity.blobType,
        resourceGroupName: entity.resourceGroupName,
        subscriptionId: entity.subscriptionId
    };
    return entity;
}


/*
 *
 *  @param {} entity
 *  @param {} context
 *
 * returns boolean if blob file exceeds max roll over days setting.
 */
function isAppendBlobArchived(context, entity) {

    if (entity.blobType._ === "AppendBlob" && entity.offset._ > 0 && entity.eventdate !== undefined) {
        var maxArchivedHours = parseInt(process.env.APPSETTING_MAX_LOG_FILE_ROLLOVER_HOURS);
        if (entity.storageName._ === "muw1olpmessagingprodsa01" || entity.storageName._ === "mue1olpmessagingprodsa01" || entity.storageName._ === "muw1olpolpadminccatsa01" || entity.storageName._ === "mue2olpolpadminccatsa01" || entity.storageName._ === "muw1bitfunctionslogssa" || entity.storageName._ === "mue1bitfunctionslogssa") {
                    maxArchivedHours = 1;
                } else if (entity.storageName._ === "mue1supportakspocsa" || entity.storageName._ === "mue1supportaksdevsa" || entity.storageName._ === "muw1nortonaksintsa" || entity.storageName._ === "muw1supportaksstgsa" || entity.storageName._ === "muw1supportaksprodsa" || entity.storageName._ === "mue2supportaksprodsa" || entity.storageName._ === "muw1supportakscoresa") {
                    maxArchivedHours = 4;
                }
        var curDate = new Date();

        var fileCreationDate = new Date(entity.eventdate._);
        var numHoursPassedSinceFileCreation = (curDate - fileCreationDate) / (1000 * 60 * 60);

        var lastEnqueLockTime;
        if (entity.lastEnqueLockTime === undefined) {
            lastEnqueLockTime =  entity.eventdate._;
        } else {
            lastEnqueLockTime = entity.lastEnqueLockTime._;
        }

        var lastEnqueTaskDate = new Date(lastEnqueLockTime);
        var numMinPassedSinceLastEnquedTask = (curDate - lastEnqueTaskDate) / (1000 * 60 * 60);
        var maxlockThresholdMin = 15;
        // Here we are also checking that file creation date should exceed threshold.
        // Also file row should not have its lock released recently this ensures those file rows do not get archived as soon as their lock is released.
        if ( (numHoursPassedSinceFileCreation >= maxArchivedHours) && (numMinPassedSinceLastEnquedTask <= maxlockThresholdMin)) {
            context.log("Archiving Append Blob File with RowKey: %s numHoursPassedSinceFileCreation: %d numMinPassedSinceLastEnquedTask: %d", entity.RowKey._, numHoursPassedSinceFileCreation, numMinPassedSinceLastEnquedTask)
            return true;
        } else {
            return false;
        }
    } else if (entity.eventdate === undefined) {
        context.log("Archiving Append Blob File with(no eventdate) RowKey: ", entity.RowKey)
        return true;
    } else {
        return false;
    }
}

/**
 * @param  {} continuationToken
 * @param  {} tableQuery
 * @param  {} newFiles
 *
 * fetches the files from FileOffset Map table recursively
 */
function queryFiles(continuationToken, tableQuery, context) {
    var allentities = [];
    return new Promise(function (resolve, reject) {
        return queryEntitiesSegmented(process.env.APPSETTING_TABLE_NAME, tableQuery, continuationToken).then(function (results) {
            continuationToken = results.continuationToken;
            results.entries.forEach(function (entity) {
                allentities.push(entity);
            });
            if (continuationToken == null) {
                // context.log("queryFiles: finished all pages.");
                return resolve(allentities);
            } else {
                //context.log("queryFiles: moving to next page.");
                return queryFiles(continuationToken, tableQuery, context).then(function (r) {
                    resolve(allentities.concat(r));
                }).catch(reject);
            }
        }).catch(reject);
    })
}

/*
 *  Updates the entities in batches, it groups the entities by partitionkey
 *  mode - if mode == insert it inserts or merges the entity
 */
function batchUpdateOffsetTable(context, allentities, mode) {
    var batch_promises = [];
    var successCnt = 0;
    var errorCnt = 0;
    var maxBatchItems = 100;
    // All entities in the batch must have the same PartitionKey
    var groupedEntities = allentities.reduce(function (rv, e) {
        (rv[e.PartitionKey._] = rv[e.PartitionKey._] || []).push(e);
        return rv;
    }, {});

    Object.keys(groupedEntities).forEach(function (groupKey) {
        var entities = groupedEntities[groupKey];
        for (let batchIndex = 0; batchIndex < entities.length; batchIndex += maxBatchItems) {
            (function (batchIndex, groupKey) {
                batch_promises.push(new Promise(function (resolve, reject) {
                    var batch = new storageOld.TableBatch();
                    var currentBatch = entities.slice(batchIndex, batchIndex + maxBatchItems);
                    for (let index = 0; index < currentBatch.length; index++) {
                        const element = currentBatch[index];
                        if (mode === "delete") {
                            batch.deleteEntity(element);
                        } else {
                            batch.insertOrMergeEntity(element);
                        }
                    }
                    tableService.executeBatch(process.env.APPSETTING_TABLE_NAME, batch,
                        function (error, result, response) {
                            if (error) {
                                context.log("Error occurred while updating offset table for batch: " + batchIndex,  error);
                                // not using reject so that all promises will get processed in promise.all
                                errorCnt += 1;
                                return resolve({status: "error"})
                            } else {
                                //context.log("Updated offset table mode: " + mode + "for batch: " + batchIndex + " groupKey: " + groupKey + " numElementinBatch: " + currentBatch.length);
                                successCnt += 1
                                return resolve({ status: "success" });
                            }
                        });
                }));
            })(batchIndex, groupKey);

        }
    });
    return Promise.all(batch_promises).then(function(results) {

        context.log("batchUpdateOffsetTable mode: " + mode + " succentCount: " + successCnt + " errorCount: " + errorCnt);
        return results;
    });
}


/*
 * @param  {} context
 *
 * It fetches the archived files for block blob type entries in table storage.
*/
function getArchivedBlockBlobFiles(context) {
    // https://azure.github.io/azure-storage-node/TableQuery.html
    // https://github.com/Azure/azure-storage-node/blob/master/lib/services/table/tableutilities.js
    var maxArchivedDays = parseInt(process.env.APPSETTING_MAX_LOG_FILE_ROLLOVER_DAYS);
    var dateVal = new Date();
    dateVal.setDate(dateVal.getDate() - maxArchivedDays);

    // fetch only Row and Partition Key for faster fetching
    var archivedFileQuery = new storageOld.TableQuery().select('PartitionKey', 'RowKey').where(' blobType eq ? and eventdate le ?date?', "BlockBlob", dateVal);
    return queryFiles(null, archivedFileQuery, context).then(function (processedFiles) {
        context.log("BlockBlob Archived Files: " + processedFiles.length);
        return processedFiles;
    }).catch(function (error) {
        // not failing so that other tasks gets archived
        context.log("Unable to fetch blockblob archived rows from table ", error);
        return [];
    });
}


/*
 * In some cases due to rogue message consumer function may not be able to process messages
 * and thus there is a chance that the file may get locked for a long time so the below function automatically
 * releases the lock after a threshold is breached.
 */
function getLockedEntitiesExceedingThreshold(context) {

    var maxlockThresholdMin = 15;
    var dateVal = new Date();
    dateVal.setMinutes(Math.max(0,dateVal.getMinutes() - maxlockThresholdMin));
    var lockedFileQuery = new storageOld.TableQuery().where(' (done eq ?) and (blobType eq ?) and (offset ge ?) and lastEnqueLockTime le ?date?', true, "AppendBlob", 0, dateVal);
    // context.log("maxlastEnqueLockTime: %s", dateVal.toISOString());
    return queryFiles(null, lockedFileQuery, context).then(function (allentities) {
        context.log("AppendBlob Locked Files exceeding maxlockThresholdMin: " + allentities.length);
        var unlockedEntities = allentities.map(function(entity) {
             context.log("Unlocking Append Blob File with RowKey: %s lastEnqueLockTime: %s", entity.RowKey._, entity.lastEnqueLockTime._);
            return getunLockedEntity(entity);
        });
        return unlockedEntities;
    }).catch(function (error) {
        context.log("Unable to fetch AppendBlob locked rows from table ", error);
        return [];
    });
}

function getFixedNumberOfEntitiesbyEnqueTime(context, entities) {
    // sort by lastenquetime they are in isoformat so are lexicographically sorted
    let lastEnqueLockTime_a;
    let lastEnqueLockTime_b;
    entities = entities.sort(function(a, b) {
        if (a.lastEnqueLockTime === undefined) {
            lastEnqueLockTime_a =  a.eventdate._;
        } else {
            lastEnqueLockTime_a = a.lastEnqueLockTime._;
        }
        if (b.lastEnqueLockTime === undefined) {
            lastEnqueLockTime_b =  b.eventdate._;
        } else {
            lastEnqueLockTime_b = b.lastEnqueLockTime._;
        }
        return lastEnqueLockTime_a - lastEnqueLockTime_b;
    });
    let filesPerStorageAccountCount = {};
    let allFileCount = 0;
    let maxFileTaskPerInvoke = 8000;
    // 800 because 25 is the max number of requests one invoke can make 25*800 = 20000 which is equal to max request per sec.
    let maxFileTaskPerInvokePerStorageAccount = 800;
    var filteredEntities = [];
    let entity = null;
    for (let idx = 0; idx < entities.length; idx += 1) {
        entity = entities[idx];

        if (filesPerStorageAccountCount[entity.storageName._] === undefined) {
            filesPerStorageAccountCount[entity.storageName._] = 1;
        } else {
            filesPerStorageAccountCount[entity.storageName._] += 1;
        }
        if (filesPerStorageAccountCount[entity.storageName._] <= maxFileTaskPerInvokePerStorageAccount) {
            allFileCount += 1;
            filteredEntities.push(entity);
        }

        if (allFileCount >= maxFileTaskPerInvoke) {
            break;
        }
    };


    return filteredEntities;
}

function setBatchSizePerStorageAccount(newFiletasks) {
    let filesPerStorageAccountCount = {};
    let task = null;
    for (let idx = 0; idx < newFiletasks.length; idx += 1) {
        task = newFiletasks[idx];
        if (filesPerStorageAccountCount[task.storageName] === undefined) {
            filesPerStorageAccountCount[task.storageName] = 1;
        } else {
            filesPerStorageAccountCount[task.storageName] += 1;
        }
    };
    let MAX_READ_API_LIMIT_PER_SEC = 10000;
    let MAX_GET_BLOB_REQUEST_PER_INVOKE = 25;
    for (let idx = 0; idx < newFiletasks.length; idx += 1) {
        task = newFiletasks[idx];
        task.batchSize = Math.min(MAX_GET_BLOB_REQUEST_PER_INVOKE, Math.floor(MAX_READ_API_LIMIT_PER_SEC/filesPerStorageAccountCount[task.storageName]))*4*1024*1024;
    }
    return newFiletasks;
}

/**
 *First it fetches the unlocked append blob files rows and creates tasks for them in service bus
 *
 * To avoid duplication of tasks all the enqueued tasks (in service bus) are marked as locked
 * This will ensure that only after consumer function releases the lock after successfully sending the log file
 * then only new task is produced for that file in case of append blobs.
 */
function getTasksForUnlockedFiles(context) {
    var existingFileQuery = new storageOld.TableQuery().where(' done eq ? and  blobType eq ? and offset ge ?', false, "AppendBlob", 0);
    return new Promise(function (resolve, reject)  {
        queryFiles(null, existingFileQuery, context).then(function (allentities) {
            var newFiletasks = [];
            var archivedFiles = [];
            var newFileEntities = [];
            var lockedEntities = [];
            allentities.forEach(function (entity) {
                if (isAppendBlobArchived(context, entity)) {
                    archivedFiles.push(entity);
                } else {
                    if (entity.storageName._ === "glo1503134026east01" || entity.storageName._ === "glo1503134026west01") {
                        newFiletasks.push(getTask(entity));
                        lockedEntities.push(getLockedEntity(entity));
                    } else {
                        newFileEntities.push(entity);
                    }
                    //context.log("Creating task for file: " + entity.RowKey._);
                }
            });
            newFileEntities = getFixedNumberOfEntitiesbyEnqueTime(context, newFileEntities)
            newFileEntities.forEach(function(entity) {
                newFiletasks.push(getTask(entity));
                lockedEntities.push(getLockedEntity(entity));
            });
            newFiletasks = setBatchSizePerStorageAccount(newFiletasks)
            context.log("New File Tasks created: " + newFiletasks.length + " AppendBlob Archived Files: " + archivedFiles.length);
            resolve([newFiletasks, archivedFiles, lockedEntities]);
        }).catch(function(error) {
            context.log("Error in getting new tasks");
            reject(error);
        });
    });
}

/**
 * @param  {} context
 *
 * Then it fetches the existing append blob file rows and creates tasks for them in service bus
 * Among the existing files it marks the ones which are inactive (it is assumed that the azure service won't be writing to this file after it switched to a new file)
 */
function PollAppendBlobFiles(context) {
    // Since this function is not synchronize it may generate duplicate task in a scenario where the same function is running concurrently
    // therefore it's best to run this function at an interval of 5-10 min
    context.bindings.tasks = []
    getTasksForUnlockedFiles(context).then(function (r) {
        var newFiletasks = r[0];
        var archivedRowEntities = r[1];
        var entitiesToUpdate = r[2];
        context.bindings.tasks = context.bindings.tasks.concat(newFiletasks);
        // context.log(newFiletasks);
        var batch_promises = [
            getLockedEntitiesExceedingThreshold(context).then(function(unlockedEntities) {
                // setting lock for new tasks and unsetting lock for old tasks
                entitiesToUpdate = entitiesToUpdate.concat(unlockedEntities);
                return batchUpdateOffsetTable(context, entitiesToUpdate, "insert");
            }),
            getArchivedBlockBlobFiles(context).then(function(blockBlobArchivedFiles) {
                // deleting both archived block blob and append blob files
                archivedRowEntities = archivedRowEntities.concat(blockBlobArchivedFiles);
                return batchUpdateOffsetTable(context, archivedRowEntities, "delete");
            })
        ];
        return Promise.all(batch_promises).then(function(results) {
            context.log("BatchUpdateResults - ", results);
            context.done();
        });
    }).catch(function (error) {
            context.log("Error in PollOffsetTable", error);
            context.done(error);
    });
}

module.exports = function (context, triggerData) {

    if (triggerData.isPastDue) {
        context.log("function is running late");
    }

    PollAppendBlobFiles(context);
};
