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
    var match = stringToWorkWith.match(regex);
    return match ? string.lastIndexOf(match.slice(-1)) : -1;
}

/*  Function to use boundary regex for azure storage accounts to avoid split issue & multiple single event issue */
function getBoundaryRegex(serviceBusTask) {
    //this boundary regex is matching for cct, llev, msgdir logs
    let logRegex = '\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}';
    // global is necessary if using regex.exec
    // https://stackoverflow.com/questions/31969913/why-does-this-regexp-exec-cause-an-infinite-loop
    var file_ext = String(serviceBusTask.blobName).split(".").pop();
    if (file_ext === "json" || file_ext === "blob") {
        logRegex = '{\\s*\"';
    }
    // uncomment and use the snippet below for overriding boundary regex for your log files
    // if (serviceBusTask.storageName === "<your storageAccountName>" || serviceBusTask.containerName === "<your containerName>" ) {
    //     logRegex = '\{\"\@timestamp\"\:\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}';
    //     logRegex = '\{\\s+\"operationName\"\:\\s+\"';
    //     logRegex = '\{\\s+\"attrs\"\:\\s+\"';
    //     logRegex = '\{\\s+\"time\"\:\\s+\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}';
    //     logRegex = '\{\\s+\"category\"\:\\s+\"';
    //     logRegex = '\{"key+';
    // }

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
    var file_ext = String(serviceBusTask.blobName).split(".").pop();
    if (suffix.length > 0) {
        if (file_ext === "json" || file_ext === "blob" || logRegex.source.startsWith('\{')) { // consider as JSON
            try {
                JSON.parse(suffix.trim());
                lastIndex = data.length;
            } catch (error) {
                context.log.verbose("Failed to parse the JSON last chunk. Ignoring:", { suffix, lastIndex, error });
            }
        } else { // consider as log
            if (suffix.endsWith('\n')) {
                lastIndex = data.length;
            }
            else {
                context.log.verbose("Failed to parse the log last chunk. Ignoring:", { suffix, lastIndex });
            }
        }
    }

    // ideally ignoredprefixLen should always be 0. it will be dropped for existing files
    // for new files offset will always start from date
    var ignoredprefixLen = Buffer.byteLength(prefix, defaultEncoding);
    // data with both prefix and suffix removed
    data = data.substring(firstIdx, lastIndex);
    // can't use matchAll since it's available only after version > 12
    let startpos = 0;
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
}

exports.decodeDataChunks = decodeDataChunks;
