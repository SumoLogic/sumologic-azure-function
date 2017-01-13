///////////////////////////////////////////////////////////////////////////////////
//                       CloudWatch Logs to SumoLogic                            //
// https://github.com/SumoLogic/sumologic-azure-function/tree/master/eventhub.js //
//////////////////////////////////////////////////////////////////////////////////

// SumoLogic Endpoint to post logs
var sumoURL = 'https://endpoint1.collection.us2.sumologic.com/receiver/v1/http/XXXXXXX';

// The following parameters override the sourceCategoryOverride, sourceHostOverride and sourceNameOverride metadata fields within SumoLogic.
// Not these can also be overridden via json within the message payload. See the README for more information.
var sourceCategoryOverride = 'none';  // If none sourceCategoryOverride will not be overridden
var sourceHostOverride = 'none';      // If none sourceHostOverride will not be set
var sourceNameOverride = 'none';      // If none sourceNameOverride will not be set

var https = require('https');
var zlib = require('zlib');
var url = require('url');


function sumoMetaKey(context, message) {
    var sourceCategory = '';
    var sourceName = '';
    var sourceHost = '';
    
    if (sourceCategoryOverride !== null && sourceCategoryOverride !== '' && sourceCategoryOverride != 'none') {
        sourceCategory = sourceCategoryOverride;
    }
    
    if (sourceHostOverride !== null && sourceHostOverride !== '' && sourceHostOverride != 'none') {
        sourceHost = sourceHostOverride;
    } else {
        // TODO: Figure out eventhub name
        // sourceHost = context.bindings['name'];
    }
    
    if (sourceNameOverride !== null && sourceNameOverride !== '' && sourceNameOverride != 'none') {
        sourceName = sourceNameOverride;
    } else {
        // TODO: Figure out eventhub name
        // sourceHost = context.bindings['name'];
    }
    
    // Ability to override metadata within the message
    // Useful within Lambda function context.log to dynamically set metadata fields within SumoLogic.
    if (message.hasOwnProperty('_sumo_metadata')) {
        var metadataOverride = message._sumo_metadata;
        if (metadataOverride.category) {
            sourceCategory = metadataOverride.category;
        }
        if (metadataOverride.host) {
            sourceHost = metadataOverride.host;
        }
        if (metadataOverride.source) {
            sourceName = metadataOverride.source;
        }
        delete message._sumo_metadata;
    }
    return sourceName + ':' + sourceCategory + ':' + sourceHost;
    
}

function postToSumo(context, messages) {
    var messagesTotal = Object.keys(messages).length;
    var messagesSent = 0;
    var messageErrors = [];
    
    var urlObject = url.parse(sumoURL);
    var options = {
        'hostname': urlObject.hostname,
        'path': urlObject.pathname,
        'method': 'POST'
    };
    
    var finalizeContext = function () {
        var total = messagesSent + messageErrors.length;
        if (total == messagesTotal) {
            context.log('messagesSent: ' + messagesSent + ' messagesErrors: ' + messageErrors.length);
            if (messageErrors.length > 0) {
                context.done('errors: ' + messageErrors);
            } else {
                context.done();
            }
        }
    };
    
    
    Object.keys(messages).forEach(function (key, index) {
        var headerArray = key.split(':');
        
        options.headers = {};
        if (headerArray[0]) {
            options.headers['X-Sumo-Name'] = headerArray[0]
        }
        if (headerArray[1]) {
            options.headers['X-Sumo-Category'] = headerArray[1]
        }
        if (headerArray[2]) {
            options.headers['X-Sumo-Host'] = headerArray[2]
        }
        
        var req = https.request(options, function (res) {
            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) {
                body += chunk;
            });
            res.on('end', function () {
                if (res.statusCode == 200) {
                    messagesSent++;
                } else {
                    errors.push('HTTP Return code ' + res.statusCode);
                }
                finalizeContext();
            });
        });
        
        req.on('error', function (e) {
            messageErrors.push(e.message);
            finalizeContext();
        });
        
        for (var i = 0; i < messages[key].length; i++) {
            if (messages[key][i].constructor === ''.constructor) {
                req.write(messages[key][i] + '\n');
            } else {
                req.write(JSON.stringify(messages[key][i]) + '\n');
            }
        }
        req.end();
    });
}


module.exports = function (context, message) {
    
    // Enables support for multiple messages in the future
    var messages_list = {};
    
    // Validate URL has been set
    var urlObject = url.parse(sumoURL);
    if (urlObject.protocol != 'https:' || urlObject.host === null || urlObject.path === null) {
        context.done('Invalid SUMO_ENDPOINT environment variable: ' + sumoURL);
    }
    
    // Remove any trailing \n
    if (message.constructor === ''.constructor) {
        message = message.replace(/\n$/, '');
        message.trim();
    }
    
    // Auto detect if message is json
    try {
        message = JSON.parse(message);
    } catch (err) {
        // Do nothing, leave as text
    }
    
    // Figure out metadata fields
    var metadataKey = sumoMetaKey(context, message);
    
    if (metadataKey in messages_list) {
        messages_list[metadataKey].push(message);
    } else {
        messages_list[metadataKey] = [message];
    }
    
    // Push messages to Sumo
    postToSumo(context, messages_list);
};