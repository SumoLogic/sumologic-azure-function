const { decodeDataChunks } = require('./decodeDataChunks');
var sumoHttp = require("./sumoclient");

function sendToSumoBlocking(chunk, sendOptions, context, isText) {

    return new Promise(function (resolve, reject) {

        function failureHandler(msgArray, ctx) {
            reject();
        }
        function successHandler(ctx) {
            resolve();
        }
        let sumoClient = new sumoHttp.SumoClient(sendOptions, context, failureHandler, successHandler);
        if (!isText) {
            // Default encoding is UTF-8
            let data = chunk.toString('utf8');
            sumoClient.addData(data);
        } else {
            sumoClient.addData(chunk);
        }

        sumoClient.flushAll();
    });

}

/*
    Creates a promise chain for each of the chunk received from decodeDataChunks
    It increments
 */
async function sendDataToSumoUsingSplitHandler(context, dataBytesBuffer, sendOptions, serviceBusTask) {

    var results = decodeDataChunks(context, dataBytesBuffer, serviceBusTask);
    var ignoredprefixLen = results[0];
    var dataChunks = results[1];
    var numChunksSent = 0;
    var dataLenSent = 0;
    return new Promise(function (resolve, reject) {

        let promiseChain = Promise.resolve();
        const makeNextPromise = (chunk) => () => {
            return sendToSumoBlocking(chunk, sendOptions, context, true).then(function () {
                numChunksSent += 1;
                dataLenSent += Buffer.byteLength(chunk);
            });
        };
        for (var i = 0; i < dataChunks.length; i++) {
            promiseChain = promiseChain.then(makeNextPromise(dataChunks[i]));
        }
        return promiseChain.catch(function (err) {
            context.log.error(`Error in sendDataToSumoUsingSplitHandler blob: ${serviceBusTask.rowKey} prefix: ${ignoredprefixLen} Sent ${dataLenSent} bytes of data. numChunksSent ${numChunksSent}`);
            resolve(dataLenSent + ignoredprefixLen);
        }).then(function () {
            if (dataChunks.length === 0) {
                context.log(`No chunks to send ${serviceBusTask.rowKey}`);
            } else if (numChunksSent === dataChunks.length) {
                context.log(`All chunks successfully sent to sumo, Blob: ${serviceBusTask.rowKey}, Prefix: ${ignoredprefixLen}, Sent ${dataLenSent} bytes of data. numChunksSent ${numChunksSent}`);
            }
            resolve(dataLenSent + ignoredprefixLen);
        });

    });

}

exports.sendDataToSumoUsingSplitHandler = sendDataToSumoUsingSplitHandler;