/*
    return index of first time when pattern matches the string
 */
function regexIndexOf(string, regex, startpos) {
    var indexOf = string.substring(startpos || 0).search(regex);
    return (indexOf >= 0) ? (indexOf + (startpos || 0)) : indexOf;
}

/*
    return index of last time when pattern matches the string
 */
function regexLastIndexOf(string, regex, startpos) {
    // https://stackoverflow.com/questions/19445994/javascript-string-search-for-regex-starting-at-the-end-of-the-string
    var stringToWorkWith = string.substring(startpos, string.length);
    var match = string.match(regex);
    return match ? string.lastIndexOf(match.slice(-1)) : -1;
}

/*  Function to use boundary regex for azure storage accounts to avoid split issue & multiple single event issue */
function getBoundaryRegex(serviceBusTask) {
    //this boundary regex is matching for cct, llev, msgdir logs
    let logRegex = '\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}';
    // global is necessary if using regex.exec
    // https://stackoverflow.com/questions/31969913/why-does-this-regexp-exec-cause-an-infinite-loop
    var file_ext = String(serviceBusTask.blobName).split(".").pop();
    if (file_ext === "json") {
        logRegex = '\{\\s*\"';
    }
    if (serviceBusTask.storageName === "mue1supportakspocsa" || serviceBusTask.storageName === "mue1supportaksdevsa" || serviceBusTask.storageName === "muw1nortonaksintsa" || serviceBusTask.storageName === "muw1supportaksstgsa" || serviceBusTask.storageName === "muw1supportaksprodsa" || serviceBusTask.storageName === "mue2supportaksprodsa" || serviceBusTask.storageName === "muw1supportakscoresa") {
        if (file_ext === "log") {
            logRegex = '\{\"\@timestamp\"\:\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}';
        } else if (file_ext === "json") {
            if (serviceBusTask.containerName === "insights-logs-kube-audit") {
                logRegex = '\{\\s+\"operationName\"\:\\s+\"';
            }
            else {
                logRegex = '\{\\s+\"attrs\"\:\\s+\"';
            }
        }
    } else if (serviceBusTask.storageName === "muw1bitfunctionslogssa" || serviceBusTask.storageName === "mue1bitfunctionslogssa") {
        logRegex = '\{\\s+\"time\"\:\\s+\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}';;
    } else if ((serviceBusTask.storageName === "muw1olpolpadminccatsa01" || serviceBusTask.storageName === "mue2olpolpadminccatsa01" || serviceBusTask.storageName === "muw1olpolpadminsa01") && serviceBusTask.containerName === "insights-logs-appservicehttplogs") {
        logRegex = '\{\\s+\"category\"\:\\s+\"';
    } else if (serviceBusTask.storageName === "testsa070524101434") { // unit test
        if (file_ext === "json") {
            logRegex = '\{"key+';
        } else {
            logRegex = 'key+';
        }
    }

    return new RegExp(logRegex, "gim");
}

/*
    Removes the prefix and suffix matching the boundary.
    For example if data = "prefixwithnodate<date><msg><date><msg><date><incompletemsg>"
    then it will remove prefixwithnodate and <date><incompletemsg>.
    It will iterate over index of <date> and divide the remaining string to chunks
     of maximum 1MB size.
    datalenSent is set to bytelength of prefixwithnodate<date><msg><date><msg><date>
    so that in next invocation it will pick up from <date><incompletemsg>
 */
function decodeDataChunks(context, dataBytesBuffer, serviceBusTask, maxChunkSize = 1 * 1024 * 1024) {
    try {
        var dataChunks = [];
        let defaultEncoding = "utf8";
        // If the byte sequence in the buffer data is not valid according to the provided encoding, then it is replaced by the default replacement character i.e. U+FFFD.
        var data = dataBytesBuffer.toString(defaultEncoding);
        // remove prefix before first date
        // remove suffix after last date
        let logRegex = getBoundaryRegex(serviceBusTask);

        // return -1 if not found
        let firstIdx = regexIndexOf(data, logRegex);
        let lastIndex = regexLastIndexOf(data, logRegex, firstIdx + 1);

        // data.substring method extracts the characters in a string between "start" and "end", not including "end" itself.
        let prefix = data.substring(0, firstIdx);
        // in case only one time string
        if (lastIndex === -1 && data.length > 0) {
            lastIndex = data.length;
        }
        let suffix = data.substring(lastIndex, data.length);

        function jsonParse(jsonString, count) {
            try {
                // if last chunk is parsable then make lastIndex = data.length
                if (jsonString.length > 0) {
                    JSON.parse(jsonString.trim());
                    lastIndex = count;
                }
            } catch (error) {
                context.log.verbose("Last chunk not json parsable so ignoring", suffix, lastIndex, error);
            }
        }

        jsonParse(suffix, data.length); // Call the inner function


        // ideally ignoredprefixLen should always be 0. it will be dropped for existing files
        // for new files offset will always start from date
        var ignoredprefixLen = Buffer.byteLength(prefix, defaultEncoding);
        // data with both prefix and suffix removed
        data = data.substring(firstIdx, lastIndex);
        // can't use matchAll since it's available only after version > 12
        let startpos = 0;
        //let maxChunkSize = 1 * 1024 * 1024; // 1 MB
        while ((match = logRegex.exec(data)) !== null) {

            if (match.index - startpos >= maxChunkSize) {
                dataChunks.push(data.substring(startpos, match.index));
                context.log.verbose("New chunk %d %d", startpos, match.index);
                startpos = match.index;
            }
        }
        // pushing the remaining chunk
        let lastChunk = data.substring(startpos, data.length);
        if (lastChunk.length > 0) {
            dataChunks.push(lastChunk);
        }

        context.log.verbose(`Decode Data Chunks, rowKey: ${serviceBusTask.rowKey} numChunks: ${dataChunks.length} ignoredprefixLen: ${ignoredprefixLen} suffixLen: ${Buffer.byteLength(suffix, defaultEncoding)} dataLenTobeSent: ${Buffer.byteLength(data, defaultEncoding)}`);
        return [ignoredprefixLen, dataChunks];
    } catch (error) {
        context.log.error(`Error in decodeDataChunks blob: ${serviceBusTask.rowKey}`, error)
        return [0,0];
    }
}

exports.decodeDataChunks = decodeDataChunks;
