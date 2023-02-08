/*jshint esversion: 6 */
/**
 * Created by duc on 7/3/17.
 */

/**
 * Class containing default data transformer
 * @constructor
 */
function Transformer(context) {
    this.context = context;
}

Transformer.prototype.azureAudit = function (data) {

    var finalResult = [];
    if (data instanceof Array) {
        data.forEach( message => {
            if (message.records instanceof Array) {
                message.records.forEach(subMsg => {
                    finalResult.push(subMsg);
                });
            } else {
                finalResult.push(message);
            }
        });
    } else {
        if (data.records instanceof Array) {
            data.records.forEach(subMsg => { finalResult.push(subMsg);});
        } else { finalResult.push(data);}
    }
    return finalResult;
};

/**
 * Function to generate metric objects from a single Azure metric object
 * @param msg the original Azure metric object
 * @selectStatsForMetricFn: function to return a set of statistics for a given Azure Metric raw record
 * @returns {Array} of metric objects each contain these fields: 'metric','timestamp','statistic','value' and other keys the original Azure metric object, MINUS 'metricName','time', and 5 statistics.
 * These objects, however are not ready to be sent to the SumoMetricClient. They have to be formatted via formatMetricObject function before being forwarded to a SumoMetricClient
 */
Transformer.prototype.generateRawMetricObjectsFromAzureRawMetricRecord = function(msg, selectStatsForMetricFn) {
    let finalMetricArray = [];
    var self = this;
    if (msg.SourceSystem && msg.SourceSystem === "Insights") {
        let newDatapoint = Object.assign({}, msg);
        // need to convert to epoch seconds
        newDatapoint['timestamp'] = Math.ceil((new Date(msg.TimeGenerated)).getTime()/1000);
        newDatapoint['metric'] = msg.Name;
        newDatapoint['namespace'] = msg.Namespace;
        newDatapoint['value'] = msg.Val;
        try {
            tagObj = JSON.parse(msg.Tags);
            newDatapoint = Object.assign(newDatapoint, tagObj);
            // self.context.log(tagObj);
            // self.context.log(newDatapoint);
        } catch (e) {
            self.context.log("Error in TagsParsing", e);
        }
        delete newDatapoint.Tags
        delete newDatapoint.Val;
        delete newDatapoint.Namespace;
        delete newDatapoint.Name;
        delete newDatapoint.TimeGenerated;
        finalMetricArray.push(newDatapoint);
    } else {
        let stat_method;
        // build core component
        let core = Object.assign({},msg);
        delete core.count;
        delete core.total;
        delete core.average;
        delete core.maximum;
        delete core.minimum;
        delete core.metricName;
        delete core.time;
        // need to convert to epoch seconds
        core['timestamp'] = Math.ceil((new Date(msg.time)).getTime()/1000);
        core['metric'] = msg['metricName'];
        for (stat_method of selectStatsForMetricFn(msg)) {
            if (msg && stat_method in msg) {
                // in case some metrics don't have this statistic
                let newDatapoint = Object.assign({},core);
                newDatapoint['statistic']=stat_method;
                newDatapoint['value']=msg[stat_method];
                finalMetricArray.push(newDatapoint);
            }
        }
    }


    return finalMetricArray
}

Transformer.prototype.getMetricKeyValue = function(key, metricObject) {
    let kv = ""
    if (metricObject && metricObject[key] && (typeof metricObject[key] === "string") && (metricObject[key].indexOf(" ") > -1)) {
        // replacing string values with spaces with quotes
        kv = key + '=' + metricObject[key].replace(/\s/g, "_");
    } else {
        kv = key + '=' + metricObject[key];
    }
    return kv;
}


/**
 * Function to prepare a metric object to be ready to be digested by a SumoMetricClient.
 * @param metricObject: The metric object to be modified. Each object must have a 'metric', 'value','time' key and the rest are treated as extra tags
 * @param format 'graphite' or 'carbon20'
 * @return A metric object that has a field 'metric_string' that will be sent to Sumo via a SumoMetricClient, along with any other metric and non-metric metadata
 */
Transformer.prototype.prepareMetricObject = function(metricObject,format) {
    if (metricObject['metric_string']) return metricObject; // do nothing

    if (format.toLocaleLowerCase() == "graphite") {
        // graphite's is more complicated because we need to pass in dimension fields in the header via _sumo_meta_data field for each metric object.

        let metric_string = metricObject.metric + ' ' + metricObject.value + ' ' + metricObject.timestamp;
        // now delete these fields, then put the rest of the stuff, if any inside X-Sumo-Dimensions via the msg's _sumo_meta_data key
        delete metricObject.metric;
        delete metricObject.value;
        delete metricObject.timestamp;
        let meta_data = {};
        // first get the existing _sumo_meta_data key, if any
        if ('_sumo_meta_data' in metricObject) {
            meta_data = metricObject['_sumo_meta_data'];
            delete metricObject['_sumo_meta_data']; // we will put it back later
        }
        if (Object.keys(metricObject).length > 0) {
            const msg_keys = Object.keys(metricObject);
            // now construct the Dimensions and put in _sumo_meta_data
            let dimension_string = this.getMetricKeyValue(msg_keys[0], metricObject);
            for (var i = 1; i < msg_keys.length; i++) {
                dimension_string += ',' + this.getMetricKeyValue(msg_keys[i], metricObject);
            }
            if (meta_data['X-Sumo-Dimensions'] && meta_data['X-Sumo-Dimensions'] !== '') {
                meta_data['X-Sumo-Dimensions'] += ',' + dimension_string;
            } else {
                meta_data['X-Sumo-Dimensions'] = dimension_string;
            }
        }
        // now put back the meta_data and add the metric_string
        metricObject['_sumo_meta_data'] = meta_data;
        metricObject['metric_string'] = metric_string;
    } else {
        // carbon20
        // now delete these fields, then put the rest of the stuff, if any inside X-Sumo-Dimensions via the msg's _sumo_meta_data key
        const metric_name = metricObject.metric;
        delete metricObject.metric;
        const metric_value = metricObject.value;
        delete metricObject.value;
        const metric_timestamp = metricObject.timestamp;
        delete metricObject.timestamp;
        let metric_string = '';
        let meta_data = metricObject['_sumo_meta_data'];
        delete metricObject['_sumo_meta_data'];
        // now convert all other key pair
        for (const key of Object.keys(metricObject)) {
            metric_string +=  this.getMetricKeyValue(key, metricObject) + ' ';
        }
        // this.context.log("MetricString: ", metric_string);
        // now put back other metric name, value and timestamp  the meta_data
        metric_string += 'metric=' + metric_name + '  ' + metric_value + ' ' + metric_timestamp;
        // put back meta_data if it's there
        if (meta_data) {
            metricObject['_sumo_meta_data'] = meta_data;
        }
        // finally put metric_string value in the message
        metricObject['metric_string'] = metric_string;
    }
    metricObject['metric_format'] = format;
    return metricObject;
}

/**
 * Function to generate all metric objects digestable by a SumoMetricClient from the raw Azure metric data
 * @param azureMetricArray: array of the raw Azure metric objects
 * @param selectStatsForMetricFn: function to select the statistics for a given Azure metric object to be converted to a new timeseries.
 * @param format: metric format to be sent to Sumo. As of Sep 2017, it is either 'graphite' or 'carbon20'
 * At maximum, 5 metrics (based on count, total, average, maximum,minimum values) can be generated from a single Azure original metric object.
 * @returns {Array} Array of final metric objects digestable by a SumoMetricClient. Each object will have a 'metric_string' field to be used by the client to send to Sumo
 */
Transformer.prototype.generateMetricObjectsFromAzureRawData = function(azureMetricArray,selectStatsForMetricFn,format) {
    let finalMetricArray = [];
    for (const msg of azureMetricArray)   {
        let metricArray  = this.generateRawMetricObjectsFromAzureRawMetricRecord(msg,selectStatsForMetricFn);
        for (let metricObj of metricArray) {
            finalMetricArray.push(this.prepareMetricObject(metricObj,format));
        }
    }
    return finalMetricArray;
}


module.exports = {
    Transformer:Transformer
};
