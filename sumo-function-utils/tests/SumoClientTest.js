/**
 * Created by duc on 6/30/17.
 */
var sumoFnUtils = require('../lib/mainindex');
var chai = require('chai');
var expect = chai.expect;
var mocha = require('mocha');
chai.should();

describe('SumoClientTest',function () {
    var sumoEndpoint = process.env.SUMO_ENDPOINT;
    var sumoBadEndpoint = "https://endpoint1.collection.sumologic.com/receiver/v1/http/ZaVnC4dhaV3uTKlva1XrWGAjyNWn7iC07DdLRLiMM05gblqbRUJF_fdTL1Gqq9nstr_rMABh-Tq4b7-Jg8VKCZF8skUe1rFJTnsXAATTbTkjayU_D1cx8A==";
    var testMessageCount = 10;
    var testInputMessages=[];
    var testAzureInputMessages=[];
    var testBlobCount = 3;
    this.timeout(30000);

    beforeEach(function () {
        testInputMessages =[];
        testAzureInputMessages =[];
        for (var j = 0; j<testBlobCount; j++ ) {
            let tmp_buff = [];
            for (var i = 0; i < testMessageCount; i++) {
                let sourceCat = Math.ceil(Math.random() * 10);
                let sourceName = sourceCat +1;
                let sourceHost = sourceName+1 ;
                tmp_buff.push({'_sumo_metadata': {'category': sourceCat,'sourceName':sourceName,'sourceHost':sourceHost}, 'value': i});
                testInputMessages.push({'_sumo_metadata': {'category': sourceCat,'sourceName':sourceName,'sourceHost':sourceHost}, 'value': i});
            }
            testAzureInputMessages.push({"records":tmp_buff});
        }
    });

    it('it should send raw data to Sumo', function (done) {
        // test failure
        var options = {
            'urlString': sumoEndpoint,
            'metadata': {},
            'MaxAttempts': 3,
            'RetryInterval': 3000,
            'compress_data': false
        };

        var sumoClient = new sumoFnUtils.SumoClient(options, console, sumoFnUtils.FlushFailureHandler,validate);
        sumoClient.addData(testInputMessages);
        sumoClient.flushAll();

        function validate() {
            if (sumoClient.messagesAttempted  == testMessageCount*testBlobCount) {
                // data is ready
                expect(sumoClient.messagesFailed).to.equal(0);
                expect(sumoClient.messagesSent).to.equal(testMessageCount*testBlobCount);
                done();
            }
        }
    });

    it('it should send compressed data to Sumo', function (done) {
        // test failure
        var options = {
            'urlString': sumoEndpoint,
            'metadata': {},
            'MaxAttempts': 3,
            'RetryInterval': 3000,
            'compress_data': true
        };
        var sumoClient = new sumoFnUtils.SumoClient(options, console, sumoFnUtils.FlushFailureHandler,validate);
        sumoClient.addData(testInputMessages);
        sumoClient.flushAll();

        function validate() {
            if (sumoClient.messagesAttempted === testMessageCount*testBlobCount) {
                // data is ready
                expect(sumoClient.messagesFailed).to.equal(0);
                expect(sumoClient.messagesSent).to.equal(testMessageCount*testBlobCount);
                done();
            }
        }
    });

    it('it should fail to send data to a bad Sumo endpoint', function (done) {
        // test failure
        var options = {
            'urlString': sumoBadEndpoint,
            'metadata': {},
            'BufferCapacity': 10,
            'MaxAttempts': 3,
            'RetryInterval': 3000,
            'compress_data': true
        };

        var sumoClient = new sumoFnUtils.SumoClient(options, console, validate,validate);
        for (var i = 0; i < testMessageCount; i++) {
            let sourceCat = Math.ceil(Math.random() * 10);
            sumoClient.addData({'_sumo_metadata': {'category': sourceCat}, 'value': i});
        }
        sumoClient.flushAll();

        function validate() {
            console.log("Failed to send data");
            if (sumoClient.messagesAttempted === testMessageCount) {
                expect(sumoClient.messagesSent).to.equal(0);
                expect(sumoClient.messagesFailed).to.equal(testMessageCount);
                done();
            }
        }
    });


    it('Internal timer should work with Azure Function simulation', function (done) {

        var context = {'log':console.log,'done':done};
        var options ={ 'urlString':sumoEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true,'timerinterval':1000};

        context.log(`JavaScript eventhub trigger function called for message array ${testInputMessages}`);

        sumoClient = new sumoFnUtils.SumoClient(options,context,failureHandler,successHandler);
        var transformer = new sumoFnUtils.Transformer();
        sumoClient.addData(transformer.azureAudit(testAzureInputMessages));

        // we don't call sumoClient.flushAll() here

        function failureHandler(messageArray,ctx) {
            ctx.log("Failed to send to Sumo");

            ctx.log("Attempted: "+sumoClient.messagesAttempted+ ", Out of total:"+messageArray.length);
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) context.done();
        }
        function successHandler(ctx) {
            ctx.log('Successfully sent to Sumo');
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                context.done();
            }
        }

    });

    it('Azure Function simulation should work for an array input', function (done) {
        // we simulate an Azure function via the use of a context variable. This test passes if it doesn't time out
        var context = {'log':console.log,'done':done};

        var options ={ 'urlString':sumoEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true};

        context.log(`JavaScript eventhub trigger function called for message array ${testInputMessages}`);

        sumoClient = new sumoFnUtils.SumoClient(options,context,failureHandler,successHandler);
        var transformer = new sumoFnUtils.Transformer();
        sumoClient.addData(transformer.azureAudit(testAzureInputMessages));

        function failureHandler(messageArray,ctx) {
            ctx.log("Failed to send to Sumo");
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) context.done();
        }
        function successHandler(ctx) {
            ctx.log('Successfully sent to Sumo');
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                context.done();
            }
        }
        context.log("Flushing the rest of the buffers:");
        sumoClient.flushAll();

    });

    it('Azure Function simulation should work for a single message', function (done) {

        // we simulate an Azure function via the use of a context variable. This test passes if it doesn't time out
        var context = {'log':console.log,'done':done};

        var singleInputMessage = testAzureInputMessages[0];

        var options ={ 'urlString':sumoEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true};

        sumoClient = new sumoFnUtils.SumoClient(options,context,failureHandler,successHandler);
        var transformer = new sumoFnUtils.Transformer();
        sumoClient.addData(transformer.azureAudit(singleInputMessage));

        function failureHandler(messageArray,ctx) {
            ctx.log("Failed to send to Sumo");
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) context.done();
        }
        function successHandler(ctx) {
            ctx.log('Successfully sent to Sumo');
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                context.done();
            }
        }
        context.log("Flushing the rest of the buffers:");
        sumoClient.flushAll();

    });


    it('Azure Function simulating failing to send to Sumo should work', function (done) {

        // we simulate an Azure function via the use of a context variable. This test passes if it doesn't time out
        var context = {'log':console.log,'done':done};

        var singleInputMessage = testAzureInputMessages[0];

        var options ={ 'urlString':sumoBadEndpoint,'metadata':{}, 'MaxAttempts':3, 'RetryInterval':3000,'compress_data':true};

        sumoClient = new sumoFnUtils.SumoClient(options,context,failureHandler,successHandler);
        var transformer = new sumoFnUtils.Transformer();
        sumoClient.addData(transformer.azureAudit(singleInputMessage));

        function failureHandler(messageArray,ctx) {
            ctx.log("Failed to send to Sumo");
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) context.done();
        }
        function successHandler(ctx) {
            ctx.log('Successfully sent to Sumo');
            if (sumoClient.messagesAttempted === sumoClient.messagesReceived) {
                context.done();
            }
        }
        context.log("Flushing the rest of the buffers:");
        sumoClient.flushAll();

    });

});


