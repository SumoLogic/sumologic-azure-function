/*jshint esversion: 6 */
/**
 * Created by duc on 7/3/17.
 */

/**
 * Class containing default data transformer
 * @constructor
 */
function Transformer() {
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

module.exports = {
    Transformer:Transformer
};
