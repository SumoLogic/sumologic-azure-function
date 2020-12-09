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

function getResourcePathForActiveFile(entity) {
    // it returns the path for the directory in which only single service is writing to a single file.
    //rowkey: allbloblogseastus-insights-logs-appserviceconsolelogs-resourceId=-SUBSCRIPTIONS-C088DC46-D692-42AD-A4B6-9A542D28AD2A-RESOURCEGROUPS-SUMOAUDITCOLLECTION-PROVIDERS-MICROSOFT.WEB-SITES-HIMTEST-y=2020-m=12-d=07-h=22-m=00-PT1H.json
    // splitting by year and returning first half of rowkey
    return String(entity.RowKey).split("-y=")[0]
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
/**
 * @param  {} continuationToken
 * @param  {} tableQuery
 * Fetches the new files created in storage account
 */
function queryNewFiles(continuationToken, tableQuery, context) {
    var newFilesMap = {};
    var tasks = [];
    return new Promise(function (resolve, reject) {
        return queryEntitiesSegmented(process.env.APPSETTING_TABLE_NAME, tableQuery, continuationToken).then(function (results) {
            continuationToken = results.continuationToken;
            results.entries.forEach(function(entity) {
                task = getTask(entity);
                tasks.push(task);
                newFilesMap[getResourcePathForActiveFile(entity)] = entity;
            });
            if (continuationToken == null) {
                context.log("queryNewFiles: finished all pages.");
                return resolve([tasks, newFilesMap]);
            } else {
                context.log("queryNewFiles: moving to next page.");
                return queryNewFiles(continuationToken, tableQuery, context).then(function (r) {
                    return resolve([tasks.concat(r[0]), Object.assign(newFilesMap, r[1])]);
                }).catch(reject);
            }
        }).catch(reject)
    })
}
/**
 * @param  {} continuationToken
 * @param  {} tableQuery
 * @param  {} newFiles
 *
 * fetches the existing files to create tasks
 */
function queryExistingFiles(continuationToken, tableQuery, newFiles, context) {
    var processedFiles = [];
    var tasks = [];
    return new Promise(function (resolve, reject) {
        return queryEntitiesSegmented(process.env.APPSETTING_TABLE_NAME, tableQuery, continuationToken).then(function (results) {
            continuationToken = results.continuationToken;
            results.entries.forEach(function (entity) {
                task = getTask(entity);
                tasks.push(task);
                if (getResourcePathForActiveFile(entity) in newFiles) {
                    entity.done = true;
                    processedFiles.push(entity);
                }
            });
            if (continuationToken == null) {
                context.log("queryExistingFiles: finished all pages.");
                return resolve([tasks, processedFiles]);
            } else {
                context.log("queryExistingFiles: moving to next page.");
                return queryExistingFiles(continuationToken, tableQuery, newFiles, context).then(function (r) {
                    return resolve([tasks.concat(r[0]), processedFiles.concat(r[1])]);
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
function setFinishedFilesAsInactive(allfinishedFiles, context) {
    var batch_promises = [];
    var successCnt = 0;
    var errorCnt = 0;
    // All entities in the batch must have the same PartitionKey
    var groupedfinishedFiles = allfinishedFiles.reduce(function (rv, e) {
        (rv[e.PartitionKey._] = rv[e.PartitionKey._] || []).push(e);
        return rv;
    }, {});

    Object.keys(groupedfinishedFiles).forEach(function (groupKey) {
        var finishedFiles = groupedfinishedFiles[groupKey];

        for (let batchIndex = 0; batchIndex < finishedFiles.length; batchIndex += 100) {
            (function (batchIndex, groupKey) {
                batch_promises.push(new Promise(function (resolve, reject) {
                    var batch = new storage.TableBatch();
                    var currentBatch = finishedFiles.slice(batchIndex, batchIndex + 100);
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
        context.log("setFinishedFilesAsInactive succentCount: " + successCnt + " errorCount: " + errorCnt);
        context.done();
    });
}
/**
 * @param  {} context
 *
 * First it fetches the new append blob file rows (the ones with negative offset) and creates tasks for them in service bus
 * Then it fetches the existing append blob file rows and creates tasks for them in service bus
 * Among the existing files it marks the ones which are inactive (it is assumed that the azure service won't be writing to this file after it switched to a new file)
 */
function createTasks(context) {
    var newFileQuery = new storage.TableQuery().where(' done eq ? and  blobType eq ? and offset eq ?', false, "AppendBlob", 0)
    var existingFileQuery = new storage.TableQuery().where(' done eq ? and  blobType eq ? and offset gt ?', false, "AppendBlob", 0);
    return queryNewFiles(null,  newFileQuery, context).catch(function (error) {
        context.log("Unable to get new files", error);
        return [[], {}];
    }).then(function (r) {
        var newFiletasks = r[0];
        var newFilesMap = r[1];
        context.log("Found new files: ", Object.keys(newFilesMap).length);
        return queryExistingFiles(null, existingFileQuery, newFilesMap, context).then(function (res) {
            var oldFiletasks = res[0];
            var processedFiles = res[1];
            context.log("New File Tasks created: " + newFiletasks.length + " Old File Tasks created: " + oldFiletasks.length + " Existing Files: " + processedFiles.length);
            context.bindings.tasks = newFiletasks.concat(oldFiletasks);
            // not failing in case of batch updates
            return setFinishedFilesAsInactive(processedFiles, context);
        }).catch(function (error) {
                context.log("Unable to process files from table ", error);
                context.done(error);
        })
    });
}

module.exports = function (context, triggerData) {

    if (triggerData.isPastDue) {
        context.log("function is running late");
    }
    createTasks(context);
};
