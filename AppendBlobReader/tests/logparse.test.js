// sample.test.js
const parse = require('./parse');

var serviceBusTask = {
    partitionKey: 'testcontainer-22-04-24-15-40-14',
    rowKey: 'testsa220424154014-testcontainer-22-04-24-15-40-14-123.json',
    containerName: 'testcontainer-22-04-24-15-40-14',
    blobName: 'datafile.log',
    storageName: 'testsa220424154014',
    resourceGroupName: 'testsumosarg220424154014',
    subscriptionId: '',
    blobType: 'AppendBlob',
    startByte: 0,
    batchSize: 104857600
}

//T1: key1 = value1\nkey2 = value2\n
//R1: 
// 'key1 = value1\n'
test('Parse log T1 to equal R1', () => {
    regex = 'key+';
    data = 'key1 = value1\nkey2 = value2\n';
    var expectedOutPut = [
        0,
        [
            'key1 = value1\n'
        ]
    ];

    expect(parse(data, regex, serviceBusTask)).toBe(JSON.stringify(expectedOutPut));
});

//T2: value1\nkey1 = value1\nkey2 = value2\nkey4 = value4
//R2: 
// 'key1 = value1\n'
test('Parse log T2 to equal R2', () => {
    regex = 'key+';
    data = 'value1\nkey1 = value1\nkey2 = value2\nkey4 = value4';
    var expectedOutPut = [
        7,
        [
            'key1 = value1\nkey2 = value2\n'
        ]
    ];

    expect(parse(data, regex, serviceBusTask)).toBe(JSON.stringify(expectedOutPut));
});

//T3: value1\nkey1 = value1\nkey2 = value2\nkey4 =
//R3: 
// 'key1 = value1\n'
test('Parse log T3 to equal R3', () => {
    regex = 'key+';
    data = 'value1\nkey1 = value1\nkey2 = value2\nkey4 =';
    var expectedOutPut = [
        7,
        [
            'key1 = value1\nkey2 = value2\n'
        ]
    ];

    expect(parse(data, regex, serviceBusTask)).toBe(JSON.stringify(expectedOutPut));
});

//T4: \nkey1 = value1\nkey2 = value2\nke
//R4: 
// 'key1 = value1\n'
test('Parse log T4 to equal R4', () => {
    regex = 'key+';
    data = '\nkey1 = value1\nkey2 = value2\nke';
    var expectedOutPut = [
        1,
        [
            'key1 = value1\n'
        ]
    ];

    expect(parse(data, regex, serviceBusTask)).toBe(JSON.stringify(expectedOutPut));
});

//T5: \nkey2 = value2\n
//R5: []
test('Parse log T5 to equal R5', () => {
    regex = 'key+';
    data = '\nkey2 = value2\n';
    var expectedOutPut = [
        1,
        []
    ];

    expect(parse(data, regex, serviceBusTask)).toBe(JSON.stringify(expectedOutPut));
});

//T6: \nkey1 = value\\n1\nkey2 = value2\nke
//R6: key1 = value1\n
test('Parse log T6 to equal R6', () => {
    regex = 'key+';
    data = '\nkey1 = value\\n1\nkey2 = value2\nke';

    serviceBusTask.blobName = 'datafile.json';
    var expectedOutPut = [
        1,
        ['key1 = value\\n1\n']
    ];

    expect(parse(data, regex, serviceBusTask)).toBe(JSON.stringify(expectedOutPut));
});