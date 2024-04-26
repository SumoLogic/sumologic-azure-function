// Create package.json with "{"scripts": { "test": "jest" }}"
// install: npm install --save-dev jest
// run: npm test 

const parse = require('./parse');

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

    expect(parse(data, regex)).toBe(JSON.stringify(expectedOutPut));
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

    expect(parse(data, regex)).toBe(JSON.stringify(expectedOutPut));
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

    expect(parse(data, regex)).toBe(JSON.stringify(expectedOutPut));
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

    expect(parse(data, regex)).toBe(JSON.stringify(expectedOutPut));
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

    expect(parse(data, regex)).toBe(JSON.stringify(expectedOutPut));
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

    expect(parse(data, regex)).toBe(JSON.stringify(expectedOutPut));
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

    expect(parse(data, regex)).toBe(JSON.stringify(expectedOutPut));
});