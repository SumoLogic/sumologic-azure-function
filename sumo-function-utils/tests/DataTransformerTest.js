/**
 * Created by duc on 6/30/17.
 */


var transformer = require('../lib/datatransformer');
var chai = require('chai');
var expect = chai.expect;
var mocha = require('mocha')
var assert = chai.assert;
chai.should();


describe('DataTransformerTest',function () {
    var myTransformer = new transformer.Transformer();
    var testInput;
    var testMessageCount = 10;
    var testBlobCount = 3;

    beforeEach( function(){
        testInput=[];
        for (var j = 0; j<testBlobCount; j++ ) {
            var tmp_buff = [];
            for (var i = 0; i < testMessageCount; i++) {
                let sourceCat = Math.ceil(Math.random() * 10);
                tmp_buff.push({'_sumo_metadata': {'category': sourceCat}, 'value': i});
            }
            testInput.push({"records":tmp_buff});
        }
    });

    it('it should unpack a single input blob properly',function() {
        expect(myTransformer.azureAudit(testInput[0]).length).to.equal(testMessageCount);
    });

    it('it should unpack an array input blob properly',function() {
        expect(myTransformer.azureAudit(testInput).length).to.equal(testMessageCount*testBlobCount);
    });

})

