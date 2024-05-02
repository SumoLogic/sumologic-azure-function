//////////////////////////////////////////////////////////////////////////////////////////////////////
//           Function to create Append Blob tasks using File OffsetMap Table into Azure Event Hub //
//////////////////////////////////////////////////////////////////////////////////////////////////////

const { TableClient, TableTransaction } = require("@azure/data-tables");
const tableClient = TableClient.fromConnectionString(process.env.AzureWebJobsStorage, process.env.TABLE_NAME);

function getTask(entity) {
    return {
        partitionKey: entity.partitionKey,
        rowKey: entity.rowKey,
        containerName: entity.containerName,
        blobName: entity.blobName,
        storageName: entity.storageName,
        resourceGroupName: entity.resourceGroupName,
        subscriptionId: entity.subscriptionId,
        blobType: "AppendBlob",
        startByte: parseInt(entity.offset),
    };
}

function getLockedEntity(entity) {
    //a single entity group transaction is limited to 100 entities. Also, the entire payload of the transaction may not exceed 4MB
    // rowKey/partitionKey cannot contain "/"
    // lastEnqueLockTime - it denotes the last time when it was enqueued in Event Hub
    // done - it is set to true which means the task is enqueued in Event Hub
    var entity = {
        partitionKey: entity.partitionKey,
        rowKey: entity.rowKey,
        done: true,
        lastEnqueLockTime: new Date().toISOString(),
        // In a scenario where the entity could have been deleted (archived) by appendblob because of large queueing time so to avoid error in insertOrMerge Entity we include rest of the fields like storageName,containerName etc.
        blobName: entity.blobName,
        containerName: entity.containerName,
        storageName: entity.storageName,
        offset: { type: "Int64", value: String(entity.offset) },
        blobType: entity.blobType,
        resourceGroupName: entity.resourceGroupName,
        subscriptionId: entity.subscriptionId
    };
    return entity;
}

function getunLockedEntity(entity) {
    //a single entity group transaction is limited to 100 entities. Also, the entire payload of the transaction may not exceed 4MB
    // rowKey/partitionKey cannot contain "/"
    // lastEnqueLockTime - it denotes the last time when it was enqueued in Event Hub
    // done - it is set to true which means the task is enqueued in Event Hub
    var lastEnqueLockTime;
    if (entity.lastEnqueLockTime === undefined) {
        lastEnqueLockTime = entity.eventdate;
    } else {
        lastEnqueLockTime = entity.lastEnqueLockTime;
    }
    var entity = {
        partitionKey: entity.partitionKey,
        rowKey: entity.rowKey,
        done: false,
        // In a scenario where the entity could have been deleted (archived) by appendblob because of large queueing time so to avoid error in insertOrMerge Entity we include rest of the fields like storageName,containerName etc.
        lastEnqueLockTime: lastEnqueLockTime,
        eventdate: entity.eventdate,
        blobName: entity.blobName,
        containerName: entity.containerName,
        storageName: entity.storageName,
        offset: { type: "Int64", value: String(entity.offset) },
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

    if (entity.blobType === "AppendBlob" && entity.offset > 0 && entity.eventdate !== undefined) {
        var maxArchivedHours = parseInt(process.env.APPSETTING_MAX_LOG_FILE_ROLLOVER_HOURS);
        if (entity.storageName === "muw1olpmessagingprodsa01" || entity.storageName === "mue1olpmessagingprodsa01" || entity.storageName === "muw1olpolpadminccatsa01" || entity.storageName === "mue2olpolpadminccatsa01" || entity.storageName === "muw1bitfunctionslogssa" || entity.storageName === "mue1bitfunctionslogssa") {
            maxArchivedHours = 1;
        } else if (entity.storageName === "mue1supportakspocsa" || entity.storageName === "mue1supportaksdevsa" || entity.storageName === "muw1nortonaksintsa" || entity.storageName === "muw1supportaksstgsa" || entity.storageName === "muw1supportaksprodsa" || entity.storageName === "mue2supportaksprodsa" || entity.storageName === "muw1supportakscoresa") {
            maxArchivedHours = 4;
        }
        var curDate = new Date();

        var fileCreationDate = new Date(entity.eventdate);
        var numHoursPassedSinceFileCreation = (curDate - fileCreationDate) / (1000 * 60 * 60);

        var lastEnqueLockTime;
        if (entity.lastEnqueLockTime === undefined) {
            lastEnqueLockTime = entity.eventdate;
        } else {
            lastEnqueLockTime = entity.lastEnqueLockTime;
        }

        var lastEnqueTaskDate = new Date(lastEnqueLockTime);
        var numMinPassedSinceLastEnquedTask = (curDate - lastEnqueTaskDate) / (1000 * 60 * 60);
        var maxlockThresholdMin = 15;
        // Here we are also checking that file creation date should exceed threshold.
        // Also file row should not have its lock released recently this ensures those file rows do not get archived as soon as their lock is released.
        if ((numHoursPassedSinceFileCreation >= maxArchivedHours) && (numMinPassedSinceLastEnquedTask <= maxlockThresholdMin)) {
            context.log("Archiving Append Blob File with rowKey: %s numHoursPassedSinceFileCreation: %d numMinPassedSinceLastEnquedTask: %d", entity.rowKey, numHoursPassedSinceFileCreation, numMinPassedSinceLastEnquedTask)
            return true;
        } else {
            return false;
        }
    } else if (entity.eventdate === undefined) {
        context.log("Archiving Append Blob File with(no eventdate) rowKey: ", entity.rowKey)
        return true;
    } else {
        return false;
    }
}

/**
 * @param  {} tableQuery
 * fetches the files from FileOffset Map table recursively
 */
function queryFiles(tableQuery, context) {
    var allentities = [];
    return new Promise(async (resolve, reject) => {
        try {
            var entities = tableClient.listEntities({
                queryOptions: { filter: tableQuery }
            });

            for await (const entity of entities) {
                allentities.push(entity);
            }

            resolve(allentities);
        } catch (error) {
            context.log.error(`Error while fetching queryFiles: ${JSON.stringify(error)}`);
            reject(error);
        }
    })
}

/*
 *  Updates the entities in batches, it groups the entities by partitionKey
 *  mode - if mode == insert it inserts or merges the entity
 */
function batchUpdateOffsetTable(context, allentities, mode) {
    var batch_promises = [];
    var successCnt = 0;
    var errorCnt = 0;
    var maxBatchItems = 100;
    // All entities in the batch must have the same partitionKey
    var groupedEntities = allentities.reduce(function (rv, e) {
        (rv[e.partitionKey] = rv[e.partitionKey] || []).push(e);
        return rv;
    }, {});

    Object.keys(groupedEntities).forEach(function (groupKey) {
        var entities = groupedEntities[groupKey];
        for (let batchIndex = 0; batchIndex < entities.length; batchIndex += maxBatchItems) {
            (function (batchIndex, groupKey) {
                batch_promises.push(new Promise(async function (resolve, reject) {
                    var currentBatch = entities.slice(batchIndex, batchIndex + maxBatchItems);
                    var transaction = new TableTransaction();
                    for (let index = 0; index < currentBatch.length; index++) {
                        const element = currentBatch[index];
                        if (mode === "delete") {
                            transaction.deleteEntity(element);
                        } else {
                            transaction.updateEntity(element, "Merge");
                        }
                    }
                    try {
                        await tableClient.submitTransaction(transaction.actions);
                        successCnt += 1
                        return resolve({ status: "success" });
                    } catch (error) {
                        context.log.error(`Error occurred while updating offset table for batch: ${batchIndex}, error: ${JSON.stringify(error)}`);
                        // not using reject so that all promises will get processed in promise.all
                        errorCnt += 1;
                        return resolve({ status: "error" })
                    }
                }));
            })(batchIndex, groupKey);

        }
    });
    return Promise.all(batch_promises).then(function (results) {
        context.log.verbose("batchUpdateOffsetTable mode: " + mode + " succentCount: " + successCnt + " errorCount: " + errorCnt);
        return results;
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
    dateVal.setMinutes(Math.max(0, dateVal.getMinutes() - maxlockThresholdMin));
    var lockedFileQuery = `done eq ${true} and blobType eq '${'AppendBlob'}' and offset ge ${0} and lastEnqueLockTime le datetime'${dateVal.toISOString()}'`
    return queryFiles(lockedFileQuery, context).then(function (allentities) {
        context.log("AppendBlob Locked Files exceeding maxlockThresholdMin: " + allentities.length);
        var unlockedEntities = allentities.map(function (entity) {
            context.log("Unlocking Append Blob File with rowKey: %s lastEnqueLockTime: %s", entity.rowKey, entity.lastEnqueLockTime);
            return getunLockedEntity(entity);
        });
        return unlockedEntities;
    }).catch(function (error) {
        context.log.error(`Unable to fetch AppendBlob locked rows from table, Error: ${JSON.stringify(error)}`);
        return [];
    });
}

function getFixedNumberOfEntitiesbyEnqueTime(context, entities) {
    // sort by lastenquetime they are in isoformat so are lexicographically sorted
    let lastEnqueLockTime_a;
    let lastEnqueLockTime_b;
    entities = entities.sort(function (a, b) {
        if (a.lastEnqueLockTime === undefined) {
            lastEnqueLockTime_a = a.eventdate;
        } else {
            lastEnqueLockTime_a = a.lastEnqueLockTime;
        }
        if (b.lastEnqueLockTime === undefined) {
            lastEnqueLockTime_b = b.eventdate;
        } else {
            lastEnqueLockTime_b = b.lastEnqueLockTime;
        }
        return lastEnqueLockTime_a - lastEnqueLockTime_b;
    });
    let filesPerStorageAccountCount = {};
    let allFileCount = 0;
    let maxFileTaskPerInvoke = 8000;
    // 800 because 25 is the max number of requests one invoke can make 25*800 = 20000 which is equal to max request per sec.
    let maxFileTaskPerInvokePerStorageAccount = 8000;
    var filteredEntities = [];
    let entity = null;
    for (let idx = 0; idx < entities.length; idx += 1) {
        entity = entities[idx];

        if (filesPerStorageAccountCount[entity.storageName] === undefined) {
            filesPerStorageAccountCount[entity.storageName] = 1;
        } else {
            filesPerStorageAccountCount[entity.storageName] += 1;
        }
        if (filesPerStorageAccountCount[entity.storageName] <= maxFileTaskPerInvokePerStorageAccount) {
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
        task.batchSize = Math.min(MAX_GET_BLOB_REQUEST_PER_INVOKE, Math.floor(MAX_READ_API_LIMIT_PER_SEC / filesPerStorageAccountCount[task.storageName])) * 4 * 1024 * 1024;
    }
    return newFiletasks;
}

/**
 *First it fetches the unlocked append blob files rows and creates tasks for them in Event Hub
 *
 * To avoid duplication of tasks all the enqueued tasks (in Event Hub) are marked as locked
 * This will ensure that only after consumer function releases the lock after successfully sending the log file
 * then only new task is produced for that file in case of append blobs.
 */
function getTasksForUnlockedFiles(context) {

    var maxIngestionDelayPerFile = 5;
    var dateVal = new Date();
    dateVal.setMinutes(Math.max(0, dateVal.getMinutes() - maxIngestionDelayPerFile));
    // fetching unlocked files which were not enqueued in last 5 minutes
    var existingFileQuery = `done eq ${false} and blobType eq '${'AppendBlob'}' and offset ge ${0} and lastEnqueLockTime le datetime'${dateVal.toISOString()}'`
    return new Promise(function (resolve, reject) {
        queryFiles(existingFileQuery, context).then(function (allentities) {
            var newFiletasks = [];
            var archivedFiles = [];
            var newFileEntities = [];
            var lockedEntities = [];
            allentities.forEach(function (entity) {
                if (isAppendBlobArchived(context, entity)) {
                    archivedFiles.push(entity);
                } else {
                    if (entity.storageName === "glo1503134026east01" || entity.storageName === "glo1503134026west01") {
                        newFiletasks.push(getTask(entity));
                        lockedEntities.push(getLockedEntity(entity));
                    } else {
                        newFileEntities.push(entity);
                    }
                    context.log.verbose("Creating task for file: " + entity.rowKey);
                }
            });
            newFileEntities = getFixedNumberOfEntitiesbyEnqueTime(context, newFileEntities)
            newFileEntities.forEach(function (entity) {
                newFiletasks.push(getTask(entity));
                lockedEntities.push(getLockedEntity(entity));
            });
            newFiletasks = setBatchSizePerStorageAccount(newFiletasks)
            context.log("New File Tasks created: " + newFiletasks.length + " AppendBlob Archived Files: " + archivedFiles.length);
            resolve([newFiletasks, archivedFiles, lockedEntities]);
        }).catch(function (error) {
            context.log.error(`Error in getting new tasks, Error: ${JSON.stringify(error)}`);
            reject(error);
        });
    });
}

/**
 * @param  {} context
 *
 * Then it fetches the existing append blob file rows and creates tasks for them in Event Hub
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
        context.log.verbose("new file tasks", newFiletasks);
        var batch_promises = [
            getLockedEntitiesExceedingThreshold(context).then(function (unlockedEntities) {
                // setting lock for new tasks and unsetting lock for old tasks
                entitiesToUpdate = entitiesToUpdate.concat(unlockedEntities);
                return batchUpdateOffsetTable(context, entitiesToUpdate, "insert");
            }),

            batchUpdateOffsetTable(context, archivedRowEntities, "delete")
        ];
        return Promise.all(batch_promises).then(function (results) {
            context.log("BatchUpdateResults - ", results);
            context.done();
        });
    }).catch(function (error) {
        context.log.error(`Error in PollOffsetTable, Error: ${JSON.stringify(error)}`);
        context.done(error);
    });
}

module.exports = function (context, triggerData) {

    // just for ref.
    // triggerData = {
    //     schedule: { adjustForDST: true },
    //     scheduleStatus: {
    //         last: '2024-03-01T07:20:00.0173159+00:00',
    //         next: '2024-03-01T07:25:00+00:00',
    //         lastUpdated: '2024-03-01T07:20:00.0173159+00:00'
    //     },
    //     isPastDue: false
    // }

    if (triggerData.isPastDue) {
        context.log("function is running late");
    }

    PollAppendBlobFiles(context);
};
