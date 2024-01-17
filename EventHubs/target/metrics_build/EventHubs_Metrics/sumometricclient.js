/*jshint esversion: 6 */
/**
 * Created by duc on 6/30/17. This is a client for metric
 */
var https = require('node:https');
var zlib = require('node:zlib');
var url = require('node:url');

var sumoclient = require('./sumoclient');
var bucket = require('./messagebucket');
var sumoutils = require('./sumoutils.js');

var metadataMap  = {"category":"X-Sumo-Category","sourceName":"X-Sumo-Name","sourceHost":"X-Sumo-Host"};
/**
 * Class to receive metrics to a designated Sumo endpoint. Similar to the Log client is best used independently with a batch of messages so one can track the number
 * of messages sent successfully to Sumo and develop their own failure handling for those failed to be sent (out of this batch). It is of course
 * totally fine to use a single client for multiple batches for a best effort delivery option.
 * @param options contains information needed for the client including: the endpoint (via the parameter urlString),  max number of retries, a generateBucketKey function (optional)
 * @param context must support method "log".
 * @param flush_failure_callback is a callback function used to handle failures (after all attempts)
 * @param success_callback is a callback function when each batch is sent successfully to Sumo. It should contain some logic to determine all data sent to the client
 * has been attempted to sent to Sumo (either successfully or over max retries).
 * @constructor
 */
function SumoMetricClient(options, context, flush_failure_callback,success_callback) {
    this.metric_type_map = {'GRAPHITE':'graphite','CARBON20':'carbon20'};

    sumoclient.SumoClient.call(this,options,context,flush_failure_callback,success_callback)
    if ('metric_type' in options) {
        this.metric_type = options.metric_type
    } else {
        // default is graphite
        this.metric_type = this.metric_type_map.GRAPHITE
    }
};

SumoMetricClient.prototype = Object.create(sumoclient.SumoClient.prototype)

/**
 * Default method to generate a headersObj object for the bucket
 * @param message input message
 * @return a header object
 */
SumoMetricClient.prototype.generateHeaders = function(message, delete_metadata) {
    let sourceCategory = (this.options.metadata)? (this.options.metadata.category || '') :'';
    let sourceName = (this.options.metadata)? (this.options.metadata.name || ''):'' ;
    let sourceHost = (this.options.metadata)? (this.options.metadata.host || ''):'';
    let metricDimensions = (this.options.metadata)? (this.options.metadata.metricdimension || ''):'';
    let metricMetadata = (this.options.metadata)? (this.options.metadata.metricmetadata || ''):'';
    //let headerObj = {'X-Sumo-Name':sourceName, 'X-Sumo-Category':sourceCategory, 'X-Sumo-Host':sourceHost,'X-Sumo-Dimensions':metricDimensions,'X-Sumo-Metadata':metricMetadata};
    let headerObj = {'X-Sumo-Name':sourceName, 'X-Sumo-Category':sourceCategory, 'X-Sumo-Host':sourceHost, 'X-Sumo-Client': 'eventhubmetrics-azure-function'};
    if (metricDimensions !='') {
        headerObj['X-Sumo-Dimensions'] = metricDimensions;
    }
    if (metricMetadata !='') {
        headerObj['X-Sumo-Metadata'] = metricMetadata;
    }

    if (message.hasOwnProperty('_sumo_metadata')) {
        let metadataOverride = message._sumo_metadata;
        Object.getOwnPropertyNames(metadataOverride).forEach( function(property) {
            if (metadataMap[property]) {
                targetProperty = metadataMap[property];
            } else { targetProperty = property;}
            headerObj[targetProperty] = metadataOverride[property];
        });
        if (typeof delete_metadata === 'undefined' || delete_metadata) {
            delete message._sumo_metadata;
        }
    }

    if (this.options.metric_type == this.metric_type_map.CARBON20) {
        headerObj['Content-Type'] = 'application/vnd.sumologic.carbon2';
    } else {
        headerObj['Content-Type'] = 'application/vnd.sumologic.graphite';
    }

    return headerObj;
};

/**
 * For metric data, we need to extract the final string out and submit the message as text so it can be sent in the right metric format
 * @param data
 */
SumoMetricClient.prototype.addData = function(data) {
    var self = this;

    function submitMessage(message) {
        let metaKey = self.generateLogBucketKey(message);
        if (!self.dataMap.has(metaKey)) {
            self.dataMap.set(metaKey, new bucket.MessageBucket(self.generateHeaders(message)));
        }
        if ('metric_string' in message) {
            self.dataMap.get(metaKey).add(message['metric_string']);
        } else {
            self.dataMap.get(metaKey).add(message);
        }
    }

    if (data instanceof Array) {
        data.forEach(function(item,index,array) {
            self.messagesReceived +=1;
            submitMessage(item);
        });
    } else {
        self.messagesReceived +=1;
        submitMessage(data);
    }
};

/**
 * Flush a whole message bucket to sumo, compress data if needed and with up to MaxAttempts
 * @param {string} metaKey - key to identify the buffer from the internal map
 */
SumoMetricClient.prototype.flushBucketToSumo = function(metaKey) {
    let targetBuffer = this.dataMap.get(metaKey);
    var self = this;
    let curOptions = Object.assign({},this.options);

    this.context.log.verbose("Flush METRIC buffer for metaKey:"+metaKey);

    function httpSend(messageArray,data) {

        return new Promise( (resolve,reject) => {

                var req = https.request(curOptions, function (res) {
                    var body = '';
                    res.setEncoding('utf8');
                    res.on('data', function (chunk) {
                        body += chunk; // don't really do anything with body
                    });
                    res.on('end', function () {
                        console.log('Got response:'+res.statusCode);
                        if (res.statusCode == 200) {
                            self.messagesSent += messageArray.length;
                            self.messagesAttempted += messageArray.length;
                            resolve(body);
                            // TODO: anything here?
                        } else {
                            reject({'error':"statusCode: " + res.statusCode + " body: " + body,'res':null});
                        }
                        // TODO: finalizeContext();
                    });
                });

        req.on('error', function (e) {
            reject({'error':e,'res':null});
            // TODO: finalizeContext();
        });
        req.write(data);
        req.end();
    });
    }

    if (targetBuffer) {
        curOptions.headers = targetBuffer.getHeadersObject();
        let msgArray = [];
        let message;
        while (targetBuffer.getSize()>0) {
            message = targetBuffer.remove();
            if (message instanceof Object) {
                msgArray.push(JSON.stringify(message));
            } else {
                msgArray.push(message);
            }
        }

        if (curOptions.compress_data) {
            curOptions.headers['Content-Encoding'] = 'gzip';

            return zlib.gzip(msgArray.join('\n'),function(e,compressed_data){
                if (!e)  {
                    self.context.log.verbose("gzip successful");
                    sumoutils.p_retryMax(httpSend,self.MaxAttempts,self.RetryInterval,[msgArray,compressed_data])
                        .then(()=> {
                        self.context.log.verbose("Successfully sent to Sumo after "+self.MaxAttempts);
                        self.success_callback(self.context);}
                )
                .catch((err) => {
                    self.messagesFailed += msgArray.length;
                    self.messagesAttempted += msgArray.length;
                    self.context.log("Failed to send after retries: " + self.MaxAttempts + " " + JSON.stringify(err) + ' messagesAttempted: ' + self.messagesAttempted  + ' messagesReceived: ' + self.messagesReceived);
                    self.failure_callback(msgArray,self.context);
                });
                } else {
                    self.messagesFailed += msgArray.length;
                    self.messagesAttempted += msgArray.length;
                    self.context.log("Failed to gzip: " + JSON.stringify(e) + ' messagesAttempted: ' + self.messagesAttempted  + ' messagesReceived: ' + self.messagesReceived);
                    self.failure_callback(msgArray,self.context);
                }
            });
        }  else {
            //self.context.log('Send raw data to Sumo');
            return sumoutils.p_retryMax(httpSend,self.MaxAttempts,self.RetryInterval,[msgArray,msgArray.join('\n')])
                .then(()=> { self.success_callback(self.context);})
            .catch((err) => {
                self.messagesFailed += msgArray.length;
                self.messagesAttempted += msgArray.length;
                self.context.log("Failed to send after retries: " + self.MaxAttempts + " " + JSON.stringify(err) + ' messagesAttempted: ' + self.messagesAttempted  + ' messagesReceived: ' + self.messagesReceived);
                self.failure_callback(msgArray,self.context);
        });
        }
    }
};

/**
 * Default built-in callback function to handle failures. It simply logs data.
 * @param messageArray is all data failed to be sent to Sumo
 * @param ctx is the context variable that supports a log method.
 * @constructor
 */
function FlushFailureHandler (messageArray,ctx) {
    if (ctx) {ctx.log("Just  metrics locally");}
    if (messageArray instanceof Array) ctx.log(messageArray.join('\n')); else ctx.log(messageArray) ;
};

/**
 * Default built-in callback function to handle successful sent. It simply logs a success message
 * @param ctx is a context variable that supports a log method
 * @constructor
 */
function DefaultSuccessHandler(ctx) {
    ctx.log("Sent to Sumo successfully") ;
};

module.exports = {
    SumoMetricClient:SumoMetricClient,
    FlushFailureHandler:FlushFailureHandler,
    DefaultSuccessHandler:DefaultSuccessHandler
}