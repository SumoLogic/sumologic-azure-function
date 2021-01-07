///////////////////////////////////////////////////////////////////////////////////
//           Function to create tasks using EventGrid Events into Azure EventHubs               //
///////////////////////////////////////////////////////////////////////////////////

var storage = require('azure-storage');
var tableService = storage.createTableService(process.env.APPSETTING_AzureWebJobsStorage);

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
function getLockedEntity(task) {
    //a single entity group transaction is limited to 100 entities. Also, the entire payload of the transaction may not exceed 4MB
    var entGen = storage.TableUtilities.entityGenerator;
    // RowKey/Partition key cannot contain "/"
    var entity = {
        PartitionKey: entGen.String(task.containerName),
        RowKey: entGen.String(task.rowKey),
        done: entGen.Boolean(true),
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
function isArchived(context, entity) {
    var maxArchivedDays = parseInt(process.env.APPSETTING_MAX_LOG_FILE_ROLLOVER_DAYS);
    var curDate = new Date();
    var lastsenddateStr;
    if (entity.blobType._ === "AppendBlob" && entity.offset._ > 0) {
        lastsenddateStr = entity.updatedate._;
    } else if (entity.blobType._ === "BlockBlob") {
        // block blob case
        lastsenddateStr = entity.eventdate._;
    } else {
        return false;
    }

    var lastSendDate = new Date(lastsenddateStr);
    var numDays = (curDate - lastSendDate) / (1000 * 60 * 60 * 24);
    var rowKey = entity.RowKey._;
    if (numDays >= maxArchivedDays) {
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
 * fetches the existing files to create tasks
 */
function queryExistingFiles(continuationToken, tableQuery, context, shouldCreateTasks) {
    var tasks = [];
    var processedFiles = [];
    return new Promise(function (resolve, reject) {
        return queryEntitiesSegmented(process.env.APPSETTING_TABLE_NAME, tableQuery, continuationToken).then(function (results) {
            continuationToken = results.continuationToken;
            if (shouldCreateTasks) {
                results.entries.forEach(function (entity) {

                    if (isArchived(context, entity)) {
                        processedFiles.push(entity);
                    } else {
                        task = getTask(entity);
                        tasks.push(task);
                    }

                });
            }

            if (continuationToken == null) {
                context.log("queryExistingFiles: finished all pages.");
                return resolve([tasks, processedFiles]);
            } else {
                context.log("queryExistingFiles: moving to next page.");
                return queryExistingFiles(continuationToken, tableQuery, context, shouldCreateTasks).then(function (r) {
                    resolve([tasks.concat(r[0]), processedFiles.concat(r[1])]);
                }).catch(reject);
            }
        }).catch(reject);
    })
}

/**
 * @param  {} finishedFiles
 *
 * Updates the finishedfiles rows in batches and mark them as done so that in subsequent triggers these rows are not pulled.
 */
function archiveFinishedFiles(allfinishedFiles, context) {
    // https://github.com/Azure/azure-storage-node/blob/master/lib/services/table/tablebatch.js
    var batch_promises = [];
    var successCnt = 0;
    var errorCnt = 0;
    var maxBatchItems = 100;
    // All entities in the batch must have the same PartitionKey
    var groupedfinishedFiles = allfinishedFiles.reduce(function (rv, e) {
        (rv[e.PartitionKey._] = rv[e.PartitionKey._] || []).push(e);
        return rv;
    }, {});

    Object.keys(groupedfinishedFiles).forEach(function (groupKey) {
        var finishedFiles = groupedfinishedFiles[groupKey];

        for (let batchIndex = 0; batchIndex < finishedFiles.length; batchIndex += maxBatchItems) {
            (function (batchIndex, groupKey) {
                batch_promises.push(new Promise(function (resolve, reject) {
                    var batch = new storage.TableBatch();
                    var currentBatch = finishedFiles.slice(batchIndex, batchIndex + maxBatchItems);
                    for (let index = 0; index < currentBatch.length; index++) {
                        const element = currentBatch[index];
                        // batch.insertOrMergeEntity(element);
                        batch.deleteEntity(element);
                    }
                    tableService.executeBatch(process.env.APPSETTING_TABLE_NAME, batch,
                        function (error, result, response) {
                            if (error) {
                                context.log("Error occurred while updating offset table for batch: " + batchIndex,  error);
                                // not using reject so that all promises will get processed in promise.all
                                errorCnt += 1;
                                return resolve({status: "error"})
                            } else {
                                context.log("Updated offset table for batch: " + batchIndex + " groupKey: " + groupKey);
                                successCnt += 1
                                return resolve({ status: "success" });
                            }
                        });
                }));
            })(batchIndex, groupKey);

        }
    });
    return Promise.all(batch_promises).then(function(results) {
        context.log("archiveFinishedFiles succentCount: " + successCnt + " errorCount: " + errorCnt);
        context.done();
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
    var archivedFileQuery = new storage.TableQuery().select('PartitionKey', 'RowKey').where(' blobType eq ? and eventdate le ?', "BlockBlob", dateVal.toISOString());
    return queryExistingFiles(null, archivedFileQuery, context, false).then(function (res) {
        var processedFiles = res[1];
        context.log("BlockBlob Archived Files: " + processedFiles.length);
        return processedFiles;
    }).catch(function (error) {
            context.log("Unable to fetch blockblob archived rows from table ", error);
            // not failing so that other tasks gets archived
            return [];
    });
}

/* To avoid duplication of tasks all the enqueued tasks (in service bus) are marked as locked
 * This will ensure that only after consumer function releases the lock after successfully send the log file
 * then only new task is produced for that file in case of append blobs.
 */
function lockEnqueuedTasks(context, alltasks) {
    // set done = true
    allentities = alltasks.map(getLockedEntity);
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
                    var batch = new storage.TableBatch();
                    var currentBatch = entities.slice(batchIndex, batchIndex + maxBatchItems);
                    for (let index = 0; index < currentBatch.length; index++) {
                        const element = currentBatch[index];
                        batch.insertOrMergeEntity(element);
                    }
                    tableService.executeBatch(process.env.APPSETTING_TABLE_NAME, batch,
                        function (error, result, response) {
                            if (error) {
                                context.log("Error occurred while updating offset table for batch: " + batchIndex,  error);
                                // not using reject so that all promises will get processed in promise.all
                                errorCnt += 1;
                                return resolve({status: "error"})
                            } else {
                                context.log("Updated offset table for batch: " + batchIndex + " groupKey: " + groupKey);
                                successCnt += 1
                                return resolve({ status: "success" });
                            }
                        });
                }));
            })(batchIndex, groupKey);

        }
    });
    return Promise.all(batch_promises).then(function(results) {
        context.log("lockEnqueuedTasks succentCount: " + successCnt + " errorCount: " + errorCnt);
    });
}

/*
 * In some cases due to rogue message consumer function may not be able to process messages
 * and thus there is a chance that the file may get locked for a long time so the below function automatically
 * releases the lock after a threshold is breached.
 */
function unlockLongPendingTasks(context, tasks) {
    // Todo: set done = False
}
/**
 * @param  {} context
 *
 * First it fetches the new append blob file rows (the ones with negative offset) and creates tasks for them in service bus
 * Then it fetches the existing append blob file rows and creates tasks for them in service bus
 * Among the existing files it marks the ones which are inactive (it is assumed that the azure service won't be writing to this file after it switched to a new file)
 */
function createTasks(context) {

    var existingFileQuery = new storage.TableQuery().where(' done eq ? and  blobType eq ? and offset ge ?', false, "AppendBlob", 0);
    return queryExistingFiles(null, existingFileQuery, context, true).then(function (res) {
        var existingFiletasks = res[0];
        var processedFiles = res[1];
        context.log("Existing File Tasks created: " + existingFiletasks.length + " AppendBlob Archived Files: " + processedFiles.length);
        context.bindings.tasks = existingFiletasks;
        // not failing in case of batch updates
        // return lockEnqueuedTasks(context, existingFiletasks).then(function() {
            return getArchivedBlockBlobFiles(context).then(function(r) {
                processedFiles = processedFiles.concat(r);
                return archiveFinishedFiles(processedFiles, context);
            });
        // });
    }).catch(function (error) {
            context.log("Error in create tasks ", error);
            context.done(error);
    })

}

module.exports = function (context, triggerData) {

    if (triggerData.isPastDue) {
        context.log("function is running late");
    }
    createTasks(context);
};
