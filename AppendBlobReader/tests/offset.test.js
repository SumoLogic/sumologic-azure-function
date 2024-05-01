// sample.test.js
var parse = require('./parse');

var sendOptions = {
    urlString: '',
    MaxAttempts: 3,
    RetryInterval: 3000,
    compress_data: true,
    clientHeader: 'appendblobreader-azure-function',
    metadata: {
        sourceFields: '',
        sourceHost: 'testsa220424154014/testcontainer-22-04-24-15-40-14',
        sourceName: 'datafile'
    }
}

var serviceBusTask = {
    partitionKey: 'testcontainer-22-04-24-15-40-14',
    rowKey: 'testsa220424154014-testcontainer-22-04-24-15-40-14-123.json',
    containerName: 'testcontainer-22-04-24-15-40-14',
    blobName: 'datafile',
    storageName: 'testsa220424154014',
    resourceGroupName: 'testsumosarg220424154014',
    subscriptionId: 'c088dd28ad2a',
    blobType: 'AppendBlob',
    startByte: 0,
    batchSize: 104857600
}

// T1: key1 = value1\nkey2 = value2\n
// R1: 14
test.concurrent('Parse log T1 to equal R1', async () => {
    let regex = 'key+';
    let data = 'key1 = value1\nkey2 = value2\n';
    let outputData = 'key1 = value1\n';
    sendOptions.metadata.sourceName = 'datafile.log';
    serviceBusTask.blobName = 'datafile.log';

    let expectedOffset = outputData.length + serviceBusTask.startByte;

    let curdataLenSent = await parse(sendOptions, serviceBusTask, data, regex);
    let newOffset = parseInt(serviceBusTask.startByte, 10) + curdataLenSent;

    expect(newOffset).toBe(expectedOffset);
}, 10000);

//T2: \n{“key1": "value1", "abc": {"xyz": "value3"}}\r\n{"key1": "value2", "abc": {"xyz": "value4"}}\n\r
//R2: 92
test.concurrent('Parse JSON T2 to equal R2', async () => {
    let regex = '\{"key+';
    let data = '{"key1": "value1", "abc": {"xyz": "value3"}}\r\n{"key1": "value2", "abc": {"xyz": "value4"}}\n\r';
    var dataChunks = [
        '{"key1": "value1", "abc": {"xyz": "value3"}}\r\n',
        '{"key1": "value2", "abc": {"xyz": "value4"}}\n\r'
    ]

    let dataLenSent = 0;
    for (var i = 0; i < dataChunks.length; i++) {
        dataLenSent += Buffer.byteLength(dataChunks[i]);
    }
    sendOptions.metadata.sourceName = 'datafile.json';
    serviceBusTask.blobName = 'datafile.json';
    let expectedOffset = dataLenSent + serviceBusTask.startByte;

    let curdataLenSent = await parse(sendOptions, serviceBusTask, data, regex);
    let newOffset = parseInt(serviceBusTask.startByte, 10) + curdataLenSent;

    expect(newOffset).toBe(expectedOffset);
}, 10000);

//T3: {"key1": "value\\n2"}
//R3: 44
test.concurrent('Parse JSON T3 to equal R3', async () => {
    regex = '\{"key+';
    data = '{"key1": "value1", "abc": {"xyz": "value3"}}';
    var dataChunks = [
        '{"key1": "value1", "abc": {"xyz": "value3"}}'
    ]
    let dataLenSent = 0;
    for (var i = 0; i < dataChunks.length; i++) {
        dataLenSent += Buffer.byteLength(dataChunks[i]);
    }
    sendOptions.metadata.sourceName = 'datafile.json';
    serviceBusTask.blobName = 'datafile.json';
    let expectedOffset = dataLenSent + serviceBusTask.startByte;

    let curdataLenSent = await parse(sendOptions, serviceBusTask, data, regex);
    let newOffset = parseInt(serviceBusTask.startByte, 10) + curdataLenSent;

    expect(newOffset).toBe(expectedOffset);
}, 10000);

// T4: key1 = valu\\ne1\nkey2 = value2\n
// R4: 17
test.concurrent('Parse log T4 to equal R4', async () => {
    let regex = 'key+';
    let data = 'key1 = valu\\ne1\nkey2 = value2\n';
    let outputData = 'key1 = valu\\ne1\n';
    sendOptions.metadata.sourceName = 'datafile.log';
    serviceBusTask.blobName = 'datafile.log';

    let expectedOffset = outputData.length + serviceBusTask.startByte;

    let curdataLenSent = await parse(sendOptions, serviceBusTask, data, regex);
    let newOffset = parseInt(serviceBusTask.startByte, 10) + curdataLenSent;

    expect(newOffset).toBe(expectedOffset);
}, 10000);