// sample.test.js
var { sendDataToSumoUsingSplitHandler } = require('../target/consumer_build/AppendBlobTaskConsumer/sendDataToSumoUsingSplitHandler');
const context = {
    log: function (message) {
        console.log(message);
    }
};

// Extend context.log with error, warning, verbose, etc.
context.log.error = function (message) {
    console.error(`Error: ${message}`);
};

context.log.warning = function (message) {
    console.warn(`Warning: ${message}`);
};

context.log.verbose = function (message) {
    console.log(`Verbose: ${message}`);
};

var sendOptions = {
    urlString: '<sumo endpoint>',
    MaxAttempts: 3,
    RetryInterval: 3000,
    compress_data: true,
    clientHeader: 'appendblobreader-azure-function',
    metadata: {
        sourceFields: '',
        sourceHost: 'testsa220424154014/testcontainer-22-04-24-15-40-14',
        sourceName: 'datafile.json'
    }
}

var serviceBusTask = {
    partitionKey: 'testcontainer-22-04-24-15-40-14',
    rowKey: 'testsa220424154014-testcontainer-22-04-24-15-40-14-123.json',
    containerName: 'testcontainer-22-04-24-15-40-14',
    blobName: 'datafile.json',
    storageName: 'testsa070524101434',
    resourceGroupName: 'testsumosarg070524101434',
    subscriptionId: '',
    blobType: 'AppendBlob',
    startByte: 0,
    batchSize: 104857600
}

// T1: key1 = value1\nkey2 = value2\n
// R1: 14
test.concurrent('Parse log T1 to equal R1', async () => {
     
    let data = 'key1 = value1\nkey2 = value2\n';
    let outputData = 'key1 = value1\nkey2 = value2\n';
    
    sendOptions.metadata.sourceName = 'datafile.log';
    serviceBusTask.blobName = 'datafile.log';

    let expectedOffset = outputData.length + serviceBusTask.startByte;

    curoutputData = await sendDataToSumoUsingSplitHandler(context, Buffer.from(data), sendOptions, serviceBusTask);
    newOffset = parseInt(serviceBusTask.startByte, 10) + curoutputData;

    expect(newOffset).toBe(expectedOffset);
}, 10000);

//T2: \n{“key1": "value1", "abc": {"xyz": "value3"}}\r\n{"key1": "value2", "abc": {"xyz": "value4"}}\n\r
//R2: 92
test.concurrent('Parse JSON T2 to equal R2', async () => {
    
    data = '{"key1": "value1", "abc": {"xyz": "value3"}}\r\n{"key1": "value2", "abc": {"xyz": "value4"}}\n\r';
    var dataChunks = [
        '{"key1": "value1", "abc": {"xyz": "value3"}}\r\n{"key1": "value2", "abc": {"xyz": "value4"}}\n\r'
    ]

    let outputData = 0;
    for (var i = 0; i < dataChunks.length; i++) {
        outputData += Buffer.byteLength(dataChunks[i]);
    }
    sendOptions.metadata.sourceName = 'datafile.json';
    serviceBusTask.blobName = 'datafile.json';
    
    let expectedOffset = outputData + serviceBusTask.startByte;

    curoutputData = await sendDataToSumoUsingSplitHandler(context, Buffer.from(data), sendOptions, serviceBusTask);
    newOffset = parseInt(serviceBusTask.startByte, 10) + curoutputData;

    expect(newOffset).toBe(expectedOffset);
}, 10000);

// //T3: {"key1": "value\\n2"}
// //R3: 20
test.concurrent('Parse JSON T3 to equal R3', async () => {
    data = '{"key1": "value\\n2"}';
    var dataChunks = [
        '{"key1": "value\\n2"}'
    ]
    let outputData = 0;
    for (var i = 0; i < dataChunks.length; i++) {
        outputData += Buffer.byteLength(dataChunks[i]);
    }
    sendOptions.metadata.sourceName = 'datafile.json';
    serviceBusTask.blobName = 'datafile.json';

    let expectedOffset = outputData + serviceBusTask.startByte;

    curoutputData = await sendDataToSumoUsingSplitHandler(context, Buffer.from(data), sendOptions, serviceBusTask);
    newOffset = parseInt(serviceBusTask.startByte, 10) + curoutputData;

    expect(newOffset).toBe(expectedOffset);
}, 10000);

// T4: key1 = valu\ne1\nkey2 = value2\n
// R4: 17
test.concurrent('Parse log T4 to equal R4', async () => {
     
    data = 'key1 = valu\ne1\nkey2 = value2\n';
    outputData = 'key1 = valu\ne1\nkey2 = value2\n';
    
    sendOptions.metadata.sourceName = 'datafile.log';
    serviceBusTask.blobName = 'datafile.log';

    expectedOffset = outputData.length + serviceBusTask.startByte;

    curoutputData = await sendDataToSumoUsingSplitHandler(context, Buffer.from(data), sendOptions, serviceBusTask);
    newOffset = parseInt(serviceBusTask.startByte, 10) + curoutputData;

    expect(newOffset).toBe(expectedOffset);
}, 10000);
