/**
 * Created by duc on 7/12/17.
 */


/**
 * Retry with Promise
 * @param {function} fn - the function to try, should return a Promise
 * @param {int} retry - the number of retries
 * @param {number} interval - the interval in millisecs between retries
 * @param {Array} fnParams - list of params to pass to the function
 * @returns {Promise} - A promise that resolves to the final result
 */

Promise.retryMax = function(fn,retry,interval,fnParams) {
    return fn.apply(this,fnParams).catch( err => {
        return (retry>1? Promise.wait(interval).then(()=> Promise.retryMax(fn,retry-1,interval)):Promise.reject(err));
    });
}

/**
 * Promise to run after some delay
 * @param {number} delay - delay in millisecs
 * @returns {Promise}
 */
Promise.wait = function(delay) {
    return new Promise((fulfill,reject)=> {
        //console.log(Date.now());
        setTimeout(fulfill,delay||0);
    });
};

/**
 * Retry until timeout
 * @param {function} fn - function to be called, should return a promise
 * @param {number} timeLimit - time limit in millisecs
 * @param {number} interval - interval between function calls, in millisecs
 * @param {Array} fnParams - array of parameters to be passed to the function
 * @returns {Promise} - A promise that resolves to the final result
 */
Promise.retryTillTimeout = function(fn, timeLimit,interval,fnParams) {
    var startTime = Date.now();
    function mainLoop()  {
        return fn.apply(this,fnParams).catch(err => {
            return (Date.now()-startTime <= timeLimit)? Promise.wait(interval).then(() => {return mainLoop();}) : Promise.reject(err);
        });
    }
    return mainLoop();
}

/*
Promise.retryTillTimeout = function(fn, timeLimit,interval) {
    var startTime = Date.now();
    return new Promise((fulfill, reject)=> {
        var MaxTime = timeLimit;
        var finalError = {'message':'Max timeout reached.'};
        var mainLoop = function() {
            if (Date.now()-startTime > timeLimit) {
                reject(finalError);
            } else {
                fn().then((rs) => {
                    fulfill(rs);
            }).catch( (err) => {
                    finalError = err;
                console.log(" Retry at: "+Date.now());
                setTimeout(mainLoop,interval);
            });
            }
        }
        mainLoop(0);
    });
}
*/

module.exports = {
    "p_retryMax" : Promise.retryMax,
    "p_wait" : Promise.wait,
    "p_retryTillTimeout" : Promise.retryTillTimeout
}