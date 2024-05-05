// sample.test.js
const { decodeDataChunks } = require('../AppendBlobTaskConsumer/decodeDataChunks');
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
    blobName: 'datafile.json',
    storageName: 'testsa220424154014',
    resourceGroupName: 'testsumosarg220424154014',
    subscriptionId: '',
    blobType: 'AppendBlob',
    startByte: 0,
    batchSize: 104857600
}

//T1: \n{“key1": "value1", "abc": {"xyz": "value3"}}\r\n{"key1": "value2", "abc": {"xyz": "value4"}}\n\r
//R1: 
// '{"key1": "value1", "abc": {"xyz": "value3"}}\r\n',
// '{"key1": "value2", "abc": {"xyz": "value4"}}\n\r'
test('Parse JSON T1 to equal R1', () => {
    regex = '\{"key+';
    data = '\n{"key1": "value1", "abc": {"xyz": "value3"}}\r\n{"key1": "value2", "abc": {"xyz": "value4"}}\n\r';
    
    var expectedOutPut = [
        1,
        [
            '{"key1": "value1", "abc": {"xyz": "value3"}}\r\n',
            '{"key1": "value2", "abc": {"xyz": "value4"}}\n\r'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T2: {"key1": "value1"}\n{"key2": "value2"}\n{
//R2: 
// '{"key1": "value1"}\n'
test('Parse JSON T2 to equal R2', () => {
    regex = '\{"key+';
    data = '{"key1": "value1"}\n{"key2": "value2"}\n{';
    
    var expectedOutPut = [
        0,
        [
            '{"key1": "value1"}\n'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T3: "value1"}\n{"key1": "value1"}\n{"key2": "value2"}\n{"key4": "value4"
//R3: 
// '{"key1": "value1"}\n{"key2": "value2"}\n'
test('Parse JSON T3 to equal R3', () => {
    regex = '\{"key+';
    data = '"value1"}\n{"key1": "value1"}\n{"key2": "value2"}\n{"key4": "value4"';
    
    var expectedOutPut = [
        10,
        [
            '{"key1": "value1"}\n{"key2": "value2"}\n'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T4: "}\n{"key1": "value1"}\n{"key2": "value2"}\n"
//R4: 
// '{"key1": "value1"}\n{"key2": "value2"}\n'
test('Parse JSON T3 to equal R3', () => {
    regex = '\{"key+';
    data = '}\n{"key1": "value1"}\n{"key2": "value2"}\n';
    
    var expectedOutPut = [
        2,
        [
            '{"key1": "value1"}\n{"key2": "value2"}\n'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T5: "\n{"key2": "value2"}\n"
//R5: 
// '{"key2": "value2"}\n'
test('Parse JSON T5 to equal R5', () => {
    regex = '\{"key+';
    data = '\n{"key2": "value2"}\n';
    
    var expectedOutPut = [
        1,
        [
            '{"key2": "value2"}\n'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T6: "\n{“key1": "value1\n","abc": {"xyz": "value3"}}\n{"key2": "value2", "abc": {"xyz": "value3"}}"
//R6: 
// '{"key1": "value1\n","abc": {"xyz": "value3"}}\n',
// '{"key2": "value2", "abc": {"xyz": "value3"}}'
test('Parse JSON T6 to equal R6', () => {
    regex = '\{"key+';
    data = '\n{"key1": "value1\n","abc": {"xyz": "value3"}}\n{"key2": "value2", "abc": {"xyz": "value3"}}';
    
    var expectedOutPut = [
        1,
        [
            '{"key1": "value1\n","abc": {"xyz": "value3"}}\n',
            '{"key2": "value2", "abc": {"xyz": "value3"}}'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T7: "\n{"key1": "value1\n","abc": [{"xyz1":"value1"},{"xyz2":"value2"}]}\n{"key2": "value2", "abc": [{"abc1":"value1"},{"abc2":"value2"}]}"
//R7: 
// '{"key1": "value1\n","abc": [{"xyz1":"value1"},{"xyz2":"value2"}]}\n',
// '{"key2": "value2", "abc": [{"abc1":"value1"},{"abc2":"value2"}]}'
test('Parse JSON T7 to equal R7', () => {
    regex = '\{"key+';
    data = '\n{"key1": "value1\n","abc": [{"xyz1":"value1"},{"xyz2":"value2"}]}\n{"key2": "value2", "abc": [{"abc1":"value1"},{"abc2":"value2"}]}';

    var expectedOutPut = [
        1,
        [
            '{"key1": "value1\n","abc": [{"xyz1":"value1"},{"xyz2":"value2"}]}\n',
            '{"key2": "value2", "abc": [{"abc1":"value1"},{"abc2":"value2"}]}'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T8: "\n"hegjhf}{"key1": "value1", "abc": {"xyz": "value3"}}\r\n{"key1": "value2", "abc": {"xyz": "value4"}}{"ke\n\r"
//R8: 
// '{"key1": "value1", "abc": {"xyz": "value3"}}\r\n'
test('Parse JSON T7 to equal R7', () => {
    regex = '\{"key+';
    data = '\n"hegjhf}{"key1": "value1", "abc": {"xyz": "value3"}}\r\n{"key1": "value2", "abc": {"xyz": "value4"}}{"ke\n\r';

    var expectedOutPut = [
        9,
        [
            '{"key1": "value1", "abc": {"xyz": "value3"}}\r\n'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T9: "\n"hegjhf}{"key1": "value1"}\r\n{"key1": "value2", "abc": {"xyz": "value4"}}{"ke\n\r"
//R9: 
// '{"key1": "value1"}\r\n'
test('Parse JSON T7 to equal R7', () => {
    regex = '\{"key+';
    data = '\n"hegjhf}{"key1": "value1"}\r\n{"key1": "value2", "abc": {"xyz": "value4"}}{"ke\n\r';
    
    var expectedOutPut = [
        9,
        [
            '{"key1": "value1"}\r\n'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T10: "\n"hegjhf}{"key1": "value2", "abc": {"xyz": "value4"}}\r\n{"key2": "value2", "abc": {"xyz": "value4"}}{"key2\n\r"
//R10: 
// '{"key1": "value2", "abc": {"xyz": "value4"}}\r\n'
// '{"key2": "value2", "abc": {"xyz": "value4"}}'
test('Parse JSON T7 to equal R7', () => {
    regex = '\{"key+';
    data = '\n"hegjhf}{"key1": "value2", "abc": {"xyz": "value4"}}\r\n{"key2": "value2", "abc": {"xyz": "value4"}}{"key2\n\r';
    
    var expectedOutPut = [
        9,
        [
            '{"key1": "value2", "abc": {"xyz": "value4"}}\r\n',
            '{"key2": "value2", "abc": {"xyz": "value4"}}'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T11: "\n"hegjhf}{"key1": "value2", "abc": {"xyz": "value4\n"}}\r\n{"key2": "value2\n\r", "abc": {"xyz": "value4"}}{"key2\n\r"
//R11: 
// '{"key1": "value2", "abc": {"xyz": "value4\n"}}\r\n'
// '{"key2": "value2\n\r", "abc": {"xyz": "value4"}}'
test('Parse JSON T7 to equal R7', () => {
    regex = '\{"key+';
    data = '\n"hegjhf}{"key1": "value2", "abc": {"xyz": "value4\n"}}\r\n{"key2": "value2\n\r", "abc": {"xyz": "value4"}}{"key2\n\r';

    var expectedOutPut = [
        9,
        [
            '{"key1": "value2", "abc": {"xyz": "value4\n"}}\r\n',
            '{"key2": "value2\n\r", "abc": {"xyz": "value4"}}'
        ]
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T12: \n{"key1": "value2", "abc": {"xyz": "value4"}}\r\n
//R12: []
test('Parse JSON T12 to equal R12', () => {
    regex = '\{"key+';
    data = '\n{"key1": "value2", "abc": {"xyz": "value4"}}\r\n';
    
    var expectedOutPut = [
        1,
        ['{"key1": "value2", "abc": {"xyz": "value4"}}\r\n']
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T13: \n{"key1": "value2"}
//R13: '{"key1": "value2"}'
test('Parse JSON T13 to equal R13', () => {
    regex = '\{"key+';
    data = '\n{"key1": "value2"}';
    
    var expectedOutPut = [
        1,
        ['{"key1": "value2"}']
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});

//T14: \n{"key1": "value\n2"}
//R14: ''
test('Parse JSON T14 to equal R14', () => {
    regex = '\{"key+';
    data = '\n{"key1": "value\\n2"}';
    
    var expectedOutPut = [
        1,
        ['{"key1": "value\\n2"}']
    ];

    expect(JSON.stringify(decodeDataChunks(context, Buffer.from(data), serviceBusTask, maxChunkSize))).toBe(JSON.stringify(expectedOutPut));
});