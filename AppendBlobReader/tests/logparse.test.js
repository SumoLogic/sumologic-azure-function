// sample.test.js
const { decodeDataChunks } = require('../target/consumer_build/AppendBlobTaskConsumer/decodeDataChunks');
const maxChunkSize = 44;
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


var serviceBusTask = {
    partitionKey: 'testcontainer-22-04-24-15-40-14',
    rowKey: 'testsa220424154014-testcontainer-22-04-24-15-40-14-123.json',
    containerName: 'testcontainer-22-04-24-15-40-14',
    blobName: 'datafile.log',
    storageName: 'testsa070524101434',
    resourceGroupName: 'testsumosarg070524101434',
    subscriptionId: '',
    blobType: 'AppendBlob',
    startByte: 0,
    batchSize: 104857600
}

//T1: key1 = value1\nkey2 = value2\n
//R1: 
// 'key1 = value1\n'
test('Parse log T1 to equal R1', () => {
    data = 'key1 = value1\nkey2 = value2\n';
    var expectedOutPut = [
        0,
        [
            'key1 = value1\nkey2 = value2\n'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T2: value1\nkey1 = value1\nkey2 = value2\nkey4 = value4
//R2: 
// 'key1 = value1\n'
test('Parse log T2 to equal R2', () => {
    
    data = 'value1\nkey1 = value1\nkey2 = value2\nkey4 = value4';
    var expectedOutPut = [
        7,
        [
            'key1 = value1\nkey2 = value2\n'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T3: value1\nkey1 = value1\nkey2 = value2\nkey4 =
//R3: 
// 'key1 = value1\n'
test('Parse log T3 to equal R3', () => {
    
    data = 'value1\nkey1 = value1\nkey2 = value2\nkey4 =';
    var expectedOutPut = [
        7,
        [
            'key1 = value1\nkey2 = value2\n'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T4: \nkey1 = value1\nkey2 = value2\nke
//R4: 
// 'key1 = value1\n'
test('Parse log T4 to equal R4', () => {
    
    data = '\nkey1 = value1\nkey2 = value2\nke';
    var expectedOutPut = [
        1,
        [
            'key1 = value1\n'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T5: \nkey2 = value2\n
//R5: []
test('Parse log T5 to equal R5', () => {
    
    data = '\nkey2 = value2\n';
    var expectedOutPut = [
        1,
        ['key2 = value2\n']
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T6: \nkey1 = value\\n1\nkey2 = value2\nke
//R6: key1 = value1\n
test('Parse log T6 to equal R6', () => {
    
    data = '\nkey1 = value\n1\nkey2 = value2\nke';
    var expectedOutPut = [
        1,
        ['key1 = value\n1\n']
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});