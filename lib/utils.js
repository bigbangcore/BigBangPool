/**
 * Cryptonote Node.JS Pool
 * https://github.com/dvandal/cryptonote-nodejs-pool
 *
 * Utilities functions
 **/

// Load required module
let crypto = require('crypto');

let dateFormat = require('dateformat');
exports.dateFormat = dateFormat;

/**
 * Generate random instance id
 **/
exports.instanceId = function () {
    return crypto.randomBytes(4);
}

/**
 * Cleanup special characters (fix for non latin characters)
 **/
function cleanupSpecialChars(str) {
    str = str.replace(/[ÀÁÂÃÄÅ]/g, "A");
    str = str.replace(/[àáâãäå]/g, "a");
    str = str.replace(/[ÈÉÊË]/g, "E");
    str = str.replace(/[èéêë]/g, "e");
    str = str.replace(/[ÌÎÏ]/g, "I");
    str = str.replace(/[ìîï]/g, "i");
    str = str.replace(/[ÒÔÖ]/g, "O");
    str = str.replace(/[òôö]/g, "o");
    str = str.replace(/[ÙÛÜ]/g, "U");
    str = str.replace(/[ùûü]/g, "u");
    return str.replace(/[^A-Za-z0-9\-\_]/gi, '');
}
exports.cleanupSpecialChars = cleanupSpecialChars;

/**
 * Get readable hashrate
 **/
exports.getReadableHashRate = function (hashrate) {
    let i = 0;
    let byteUnits = [' H', ' KH', ' MH', ' GH', ' TH', ' PH'];
    while (hashrate > 1000) {
        hashrate = hashrate / 1000;
        i++;
    }
    return hashrate.toFixed(2) + byteUnits[i] + '/sec';
}

/**
 * Get readable coins
 **/
exports.getReadableCoins = function (coins, digits, withoutSymbol) {
    let coinDecimalPlaces = config.coinDecimalPlaces || config.coinUnits.toString().length - 1;
    let amount = ((coins || 0) / config.coinUnits).toFixed(digits || coinDecimalPlaces);
    return amount + (withoutSymbol ? '' : (' ' + config.symbol));
}

/**
 * Generate unique id
 **/
exports.uid = function () {
    let min = 100000000000000;
    let max = 999999999999999;
    let id = Math.floor(Math.random() * (max - min + 1)) + min;
    return id.toString();
};

/**
 * Ring buffer
 **/
exports.ringBuffer = function (maxSize) {
    let data = [];
    let cursor = 0;
    let isFull = false;

    return {
        append: function (x) {
            if (isFull) {
                data[cursor] = x;
                cursor = (cursor + 1) % maxSize;
            }
            else {
                data.push(x);
                cursor++;
                if (data.length === maxSize) {
                    cursor = 0;
                    isFull = true;
                }
            }
        },
        avg: function (plusOne) {
            let sum = data.reduce(function (a, b) { return a + b }, plusOne || 0);
            return sum / ((isFull ? maxSize : cursor) + (plusOne ? 1 : 0));
        },
        size: function () {
            return isFull ? maxSize : cursor;
        },
        clear: function () {
            data = [];
            cursor = 0;
            isFull = false;
        }
    };
};

/**
 * Rounding
 */
exports.toFixed = function (number, fractionDigits) {
    let coinDecimalPlaces = fractionDigits || config.coinDecimalPlaces;
    var times = Math.pow(10, coinDecimalPlaces);
    var roundNum = Math.round(number * times) / times;
    return roundNum.toFixed(coinDecimalPlaces);
}