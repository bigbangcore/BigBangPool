/**
 * Cryptonote Node.JS Pool
 * https://github.com/dvandal/cryptonote-nodejs-pool
 *
 * Pool TCP daemon
 **/

// Load required modules
let fs = require('fs');
let net = require('net');
let tls = require('tls');
let async = require('async');
let bignum = require('bignum');

let apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet, config.api);
let notifications = require('./notifications.js');
let utils = require('./utils.js');

let cnHashing = require('cryptonight-hashing');
config.hashingUtil = config.hashingUtil || false
if (config.hashingUtil)
    cnHashing = require('multi-hashing');

// Set nonce pattern - must exactly be 8 hex chars
let noncePattern = new RegExp("^[0-9A-Fa-f]{8}$");

// Set redis database cleanup interval
let cleanupInterval = config.redis.cleanupInterval && config.redis.cleanupInterval > 0 ? config.redis.cleanupInterval : 15;

// Initialize log system
let logSystem = 'pool';
require('./exceptionWriter.js')(logSystem);

let threadId = '(Thread ' + process.env.forkId + ') ';
let log = function (severity, system, text, data) {
    global.log(severity, system, threadId + text, data);
};

// Set cryptonight algorithm
let cnAlgorithm = config.cnAlgorithm || "cryptonight";
let cnVariant = config.cnVariant || 0;
let cnBlobType = config.cnBlobType || 0;

let cryptoNight;
if (!cnHashing || !cnHashing[cnAlgorithm]) {
    log('error', logSystem, 'Invalid cryptonight algorithm: %s', [cnAlgorithm]);
} else {
    cryptoNight = cnHashing[cnAlgorithm];
}

// Set instance id
let instanceId = utils.instanceId();

// Pool variables
let poolStarted = false;
let connectedMiners = {};

// Pool settings
let shareTrustEnabled = config.poolServer.shareTrust && config.poolServer.shareTrust.enabled;
let shareTrustStepFloat = shareTrustEnabled ? config.poolServer.shareTrust.stepDown / 100 : 0;
let shareTrustMinFloat = shareTrustEnabled ? config.poolServer.shareTrust.min / 100 : 0;

let banningEnabled = config.poolServer.banning && config.poolServer.banning.enabled;
let bannedIPs = {};
let perIPStats = {};

let slushMiningEnabled = config.poolServer.slushMining && config.poolServer.slushMining.enabled;

if (!config.poolServer.paymentId) config.poolServer.paymentId = {};
if (!config.poolServer.paymentId.addressSeparator) config.poolServer.paymentId.addressSeparator = "+";
if (config.poolServer.paymentId.validation == null) config.poolServer.paymentId.validation = true;

config.isRandomX = config.isRandomX || false

let previousOffset = 7
config.daemonType = config.daemonType || 'default'
if (config.daemonType === 'bytecoin')
    previousOffset = 3


// Block templates
let validBlockTemplates = [];
let currentBlockTemplate;

// Difficulty buffer,64bit
let diff1 = bignum('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF', 16);
let diff2 = bignum('FFFFFFFFFFFFFFFF', 16);

/**
 * Convert buffer to byte array
 **/
Buffer.prototype.toByteArray = function () {
    return Array.prototype.slice.call(this, 0);
};

// Variable difficulty retarget
setInterval(function () {
    let now = Date.now() / 1000 | 0;
    for (let minerId in connectedMiners) {
        let miner = connectedMiners[minerId];
        if (!miner.noRetarget) {
            miner.retarget(now);
        }
    }
}, config.poolServer.varDiff.retargetTime * 1000);//每隔xx秒重新定位一次

// Every 30 seconds clear out timed-out miners and old bans
setInterval(function () {
    let now = Date.now();
    let timeout = config.poolServer.minerTimeout * 1000;
    let timeoutMinerCount = 0;
    let outArr = [];
     
    for (let minerId in connectedMiners) {
        let miner = connectedMiners[minerId];
        if (now - miner.lastBeat > timeout) {
            log('warn', logSystem, 'Miner timed out and disconnected %s@%s', [miner.login, miner.ip]);
            delete connectedMiners[minerId];
            removeConnectedWorker(miner, 'timeout');
            outArr.push(miner.login + ':' + miner.workerName);
            timeoutMinerCount ++ ;
        }
    }

    if (timeoutMinerCount > 50){
        let now = Date.now() / 1000 | 0;
        if (outArr.length > 0){
            //TODO:USE MySQL to DO IT
            redisClient.zadd(config.coin + ':message:minerout', now, [
                now,
                JSON.stringify(outArr)
            ].join(':'), function (err, result) {
                if (err) {
                    log('error', logSystem, 'Failed inserting minerout message: %j', [err]);
                }
            });
            redisClient.expire(config.coin + ':message:minerout', now + 60 * 60 * 24);
        }  
        timeoutMinerCount = 0;      
    }
    

    if (banningEnabled) {
        // ip = ip + ':' + workName
        for (ip in bannedIPs) {
            let banTime = bannedIPs[ip];
            if (now - banTime > config.poolServer.banning.time * 1000) {
                delete bannedIPs[ip];
                delete perIPStats[ip];
                log('info', logSystem, 'Ban dropped for %s', [ip]);
            }
        }
    }
}, 30000);

/**
 * Handle multi-thread messages
 **/
process.on('message', function (message) {
    switch (message.type) {
        case 'banIP':
            log('info', logSystem, 'process.message.ip %s', [message.ip]);
            bannedIPs[message.ip] = Date.now();
            break;
    }
});

/**
 * Block template
 **/
function BlockTemplate(template, isRandomX) {
    // 在函数中通过this定义的变量就是相当于给global添加了一个属性，但是这里是在构造函数中的this，this指向它的实例
    this.isRandomX = isRandomX
    this.blob = template.data;
    this.difficulty = template.bits;
    this.height = template.prevblockheight + 1;
    if (this.isRandomX) {
        this.seed_hash = template.seed_hash;
        this.next_seed_hash = template.next_seed_hash;
    }
    this.reserveOffset = 113;
    this.buffer = Buffer.from(this.blob, 'hex');
    //instanceId.copy(this.buffer, this.reserveOffset + 4, 0, 3);
    this.previous_hash = Buffer.alloc(32);
    this.buffer.copy(this.previous_hash, 0, 8, 40);
    this.extraNonce = 0;
    this.timestamp = template.prevblocktime;

    // The clientNonceLocation is the location at which the client pools should set the nonces for each of their clients.
    this.clientNonceLocation = 109;
    // The clientPoolLocation is for multi-thread/multi-server pools to handle the nonce for each of their tiers.
    this.clientPoolLocation = 117;
}
BlockTemplate.prototype = {
    nextBlob: function () {
        this.extraNonce = parseInt(Math.random() * (1 << 30), 10);     
        let temp = parseInt(Math.round(new Date() / 1000,10));

        if(this.timestamp > temp){
            temp = this.timestamp;
        }
        this.timestamp = temp;

        this.buffer.writeUInt32BE(this.extraNonce, this.reserveOffset);
        this.buffer.writeUInt32LE(temp, 4);
        //console.log('this.buffer.timestampA:' + this.buffer.readUInt32LE(4,4));
        //console.log('this.buffer.timestampB:' + this.buffer.readUInt32LE(4,4));
        //return utils.cnUtil.convert_blob(this.buffer, cnBlobType).toString('hex');
        return this.buffer.toString('hex');
    },
    nextBlobWithChildNonce: function () {
        this.extraNonce = parseInt(Math.random() * (1 << 30), 10);
        // Write a 32 bit integer, big-endian style to the 0 byte of the reserve offset.
        this.buffer.writeUInt32BE(this.extraNonce, this.reserveOffset);
        // Don't convert the blob to something hashable.  You bad.
        return this.buffer.toString('hex');
    }
};

/**
 * Get block template
 **/
function getBlockTemplate(callback) {
    apiInterfaces.rpcDaemon('getforkheight', {}, function (error, hgt) {
        if (error) {
            log('error', logSystem, 'Error getting fork height! %j', [error]);
            callback(error, null);
            return;
        }
        apiInterfaces.rpcDaemon('getblockhash', { height: hgt }, function (error, hash) {
            if (error) {
                log('error', logSystem, 'Error getting block hash! %j', [error]);
                callback(error, null);
                return;
            }
            apiInterfaces.rpcDaemon('getwork', {
                spent: config.poolServer.spentAddress.toString(),
                privkey: config.poolServer.privkey.toString(),
                prev: hash.toString()
            }, callback);
        });
    });
}

/**
 * Process block template
 **/
function processBlockTemplate(template, isRandomX) {
    if (currentBlockTemplate)
        validBlockTemplates.push(currentBlockTemplate);

    if (validBlockTemplates.length > 3)
        validBlockTemplates.shift();
    
    //log('info',logSystem,'temp:%j',[template]);

    currentBlockTemplate = new BlockTemplate(template, isRandomX);

    for (let minerId in connectedMiners) {
        let miner = connectedMiners[minerId];
        miner.pushMessage('job', miner.getJob());// push message to miners
    }
}

/**
 * Get LastBlock Header
 **/

function getLastBlockHeader(daemon, callback) {
    apiInterfaces.rpcDaemon('getforkheight', {}, function (error, hgt) {
        if (error) {
            log('error', logSystem, 'Error getting fork height! %j', [error]);
            callback(error, true);
            return;
        }
        apiInterfaces.rpcDaemon('getblockhash', { height: hgt }, function (error, hash) {
            if (error) {
                log('error', logSystem, 'Error getting block hash! %j', [error]);
                callback(error, true);
                return;
            }
            callback(null, hash);
            return;
        });
    });
}



/**
 * Job refresh
 **/
function jobRefresh(loop) {
    async.waterfall([
        function (callback) {
            if (!poolStarted) {
                startPoolServerTcp(function (successful) { poolStarted = true });
                setTimeout(jobRefresh, 1000, loop);
                return;
            }

            getLastBlockHeader(null, function (err, res) {
                if (err) {
                    setTimeout(jobRefresh, 1000, loop);
                    return;
                }
                let LastHash = res.toString('hex');
                if (!currentBlockTemplate || LastHash !== currentBlockTemplate.previous_hash.toString('hex')) {
                    callback(null, true);
                    return;
                }
                else {
                    callback(true);
                    return;
                }
                //} else {
                //    setTimeout(jobRefresh, 3000, loop);
                //    return;
                //}
            });

        },
        function (getbc, callback) {
            let start = new Date();

            getBlockTemplate(function (err, result) {
                if (err) {
                    log('error', logSystem, 'Error polling getblocktemplate %j', [err]);
                    if (!poolStarted) log('error', logSystem, 'Could not start pool');
                    setTimeout(jobRefresh, 1000, loop);
                    return;
                }

                var work = result.work;
                let buffer = Buffer.from(work.data, 'hex');
                let new_hash = Buffer.alloc(32);

                buffer.copy(new_hash, 0, 8, 40);
                try {
                    if (!currentBlockTemplate || new_hash.toString('hex') !== currentBlockTemplate.previous_hash.toString('hex')) {
                        //log('info', logSystem, 'New %s block to mine at height %d and difficulty of %d', [config.coin, work.prevblockheight, work.bits]);
                        processBlockTemplate(work, config.isRandomX);
                        callback(null);
                        return;
                    } else {
                        callback("Duplicate Template Blocked");
                        return;
                    }
                } catch (e) { console.log(`getBlockTemplate ${e}`) }
            });
        }],
        function (err) {
            if (loop === true) {
                setTimeout(function () {
                    jobRefresh(true);
                }, config.poolServer.blockRefreshInterval);
            }
        }
    );
}

/**
 * Variable difficulty
 * 可变难度
 **/
let VarDiff = (function () {
    let variance = config.poolServer.varDiff.variancePercent / 100 * config.poolServer.varDiff.targetTime;//0.15x60=9
    return {
        variance: variance,//9
        bufferSize: config.poolServer.varDiff.retargetTime / config.poolServer.varDiff.targetTime * 4,//30/60x4=2
        tMin: config.poolServer.varDiff.targetTime - variance - 1,//60-9-1=51-1
        tMax: config.poolServer.varDiff.targetTime + variance + 1,//60+9+1=69+1
        maxJump: config.poolServer.varDiff.maxJump
    };
})();

/**
 * Miner
 **/
function Miner(id, login, pass, ip, port, agent, workerName, startingDiff, noRetarget, pushMessage) {
    this.id = id;
    this.login = login;
    this.pass = pass;
    this.ip = ip;
    this.port = port;
    this.proxy = false;
    if (agent && agent.includes('xmr-node-proxy')) {
        this.proxy = true;
    }
    this.workerName = workerName;
    this.pushMessage = pushMessage;
    this.heartbeat();
    this.noRetarget = noRetarget;
    this.difficulty = startingDiff;
    this.validJobs = [];

    // Vardiff related variables
    this.shareTimeRing = utils.ringBuffer(16);
    this.lastShareTime = Date.now() / 1000 | 0;

    if (shareTrustEnabled) {
        this.trust = {
            threshold: config.poolServer.shareTrust.threshold,
            probability: 1,
            penalty: 0
        };
    }
}
Miner.prototype = {
    retarget: function (now) {

        let options = config.poolServer.varDiff;

        let sinceLast = now - this.lastShareTime;
        let decreaser = sinceLast > VarDiff.tMax;//70

        let avg = this.shareTimeRing.avg(decreaser ? sinceLast : null);
        if (avg === null) return;
        let newDiff;

        let direction;

        if (avg > VarDiff.tMax && this.difficulty > options.minDiff) {
            newDiff = options.targetTime / avg * this.difficulty;
            newDiff = newDiff > options.minDiff ? newDiff : options.minDiff;
            direction = -1;
        }
        else if (avg < VarDiff.tMin && this.difficulty < options.maxDiff) {
            newDiff = options.targetTime / avg * this.difficulty;
            newDiff = newDiff < options.maxDiff ? newDiff : options.maxDiff;
            direction = 1;
        }
        else {
            return;
        }

        if (Math.abs(newDiff - this.difficulty) / this.difficulty * 100 > options.maxJump) {
            //let change = options.maxJump / 100 * this.difficulty * direction;
            let change = 1 * direction;
            newDiff = this.difficulty + change;
        }

        newDiff = newDiff >= currentBlockTemplate.difficulty ? currentBlockTemplate.difficulty : newDiff;

        this.setNewDiff(newDiff);

        this.shareTimeRing.clear();
        if (decreaser) this.lastShareTime = now;
    },
    setNewDiff: function (newDiff) {
        newDiff = Math.round(newDiff);
        if (this.difficulty === newDiff) return;
        //log('info', logSystem, 'Retargetting difficulty %d to %d for %s', [this.difficulty, newDiff, this.login]);
        this.pendingDifficulty = newDiff;
        this.pushMessage('job', this.getJob());
    },
    heartbeat: function () {
        this.lastBeat = Date.now();
    },
    getTargetHex: function () {
        if (this.pendingDifficulty) {
            this.lastDifficulty = this.difficulty;
            this.difficulty = this.pendingDifficulty;
            this.pendingDifficulty = null;
        }
        let buff = Buffer.from(pad((diff2.shiftRight(this.difficulty)).toString(16), 16), 'hex');
        let buffArray = buff.toByteArray().reverse();
        let buffReversed = Buffer.from(buffArray);
        this.target = buffReversed.readUInt32BE(0);
        let hex = buffReversed.toString('hex');
        return hex;
    },
    getJob: function () {
        if (this.lastBlockHeight === currentBlockTemplate.height && !this.pendingDifficulty && this.cachedJob !== null) {
            return this.cachedJob;
        }
        if (!this.proxy) {
            let blob = currentBlockTemplate.nextBlob();
            this.lastBlockHeight = currentBlockTemplate.height;
            let target = this.getTargetHex();
            let newJob = {
                id: utils.uid(),
                extraNonce: currentBlockTemplate.extraNonce,
                height: currentBlockTemplate.height,
                difficulty: this.difficulty,
                diffHex: this.diffHex,
                timestamp:currentBlockTemplate.timestamp,
                submissions: []
            };

            if (currentBlockTemplate.isRandomX) {
                newJob['seed_hash'] = currentBlockTemplate.seed_hash
                newJob['next_seed_hash'] = currentBlockTemplate.next_seed_hash
            }
            this.validJobs.push(newJob);

            while (this.validJobs.length > 4)
                this.validJobs.shift();

            this.cachedJob = {
                blob: blob,
                job_id: newJob.id,
                target: target,
                id: this.id
            };

            //log('warn', logSystem, 'cachedJob:%j', [this.cachedJob]);

            if (newJob.seed_hash) {
                this.cachedJob.seed_hash = newJob.seed_hash;
                this.cachedJob.next_seed_hash = newJob.next_seed_hash;
            }
        } else {
            let blob = currentBlockTemplate.nextBlobWithChildNonce();

            this.lastBlockHeight = currentBlockTemplate.height;
            let target = this.getTargetHex();
            //log('error', logSystem, 'itarget:%j', [target]);

            let newJob = {
                id: utils.uid(),
                extraNonce: currentBlockTemplate.extraNonce,
                height: currentBlockTemplate.height,
                difficulty: this.difficulty,
                diffHex: this.diffHex,
                clientPoolLocation: currentBlockTemplate.clientPoolLocation,
                clientNonceLocation: currentBlockTemplate.clientNonceLocation,
                submissions: []
            };

            if (currentBlockTemplate.isRandomX) {
                newJob['seed_hash'] = currentBlockTemplate.seed_hash
                newJob['next_seed_hash'] = currentBlockTemplate.next_seed_hash
            }

            this.validJobs.push(newJob);

            while (this.validJobs.length > 4)
                this.validJobs.shift();

            this.cachedJob = {
                blocktemplate_blob: blob,
                difficulty: currentBlockTemplate.difficulty,
                height: currentBlockTemplate.height,
                reserved_offset: currentBlockTemplate.reserveOffset,
                client_nonce_offset: currentBlockTemplate.clientNonceLocation,
                client_pool_offset: currentBlockTemplate.clientPoolLocation,
                target_diff: this.difficulty,
                target_diff_hex: this.diffHex,
                job_id: newJob.id,
                id: this.id
            };
            // if (newJob.seed_hash) {
            //     this.cachedJob.seed_hash = newJob.seed_hash;
            //     this.cachedJob.next_seed_hash = newJob.next_seed_hash;
            // }
        }
        if (typeof config.includeAlgo !== "undefined" && config.includeAlgo)
            this.cachedJob['algo'] = config.includeAlgo
        if (typeof config.includeHeight !== "undefined" && config.includeHeight)
            this.cachedJob['height'] = currentBlockTemplate.height
        return this.cachedJob;
    },
    checkBan: function (validShare) {
        if (!banningEnabled) return;

        // Init global per-ip shares stats
        if (!perIPStats[this.ip + ':' + this.workerName]) {
            perIPStats[this.ip + ':' + this.workerName] = { validShares: 0, invalidShares: 0 };
        }

        let stats = perIPStats[this.ip + ':' + this.workerName];
        validShare ? stats.validShares++ : stats.invalidShares++;

        if (stats.validShares + stats.invalidShares >= config.poolServer.banning.checkThreshold) {
            if (stats.invalidShares / stats.validShares >= config.poolServer.banning.invalidPercent / 100) {
                validShare ? this.validShares++ : this.invalidShares++;
                log('warn', logSystem, 'Banned %s@%s@%s', [this.login, this.ip, this.workerName]);
                bannedIPs[this.ip + ':' + this.workerName] = Date.now();
                delete connectedMiners[this.id];
                process.send({ type: 'banIP', ip: this.ip + ':' + this.workerName });
                removeConnectedWorker(this, 'banned');
            }else {
                stats.invalidShares = 0;
                stats.validShares = 0;
            }
        }
    }
};

/**
 * Handle miner method
 **/
function handleMinerMethod(method, params, ip, portData, sendReply, pushMessage) {
    let miner = connectedMiners[params.id];
    // Check for ban here, so preconnected attackers can't continue to screw you

    switch (method) {
        case 'login':
            let login = params.login;
            if (!login) {
                sendReply('Missing login');
                return;
            }

            let port = portData.port;
            let pass = params.pass;
            let workerName = '';
            if (params.rigid) {
                workerName = params.rigid.trim();
            }
            if (!workerName || workerName === '') {
                workerName = 'undefined';
            }
            workerName = utils.cleanupSpecialChars(workerName);

            if (IsBannedIp(ip +':' + workerName)) {
                sendReply('Your IP is banned, worker name is ' + workerName);
                return;
            }

            let difficulty = portData.difficulty;
            let noRetarget = false;
            if (config.poolServer.fixedDiff.enabled) {
                let fixedDiffCharPos = login.lastIndexOf(config.poolServer.fixedDiff.addressSeparator);
                if (fixedDiffCharPos !== -1 && (login.length - fixedDiffCharPos < 32)) {
                    diffValue = login.substr(fixedDiffCharPos + 1);
                    difficulty = parseInt(diffValue);
                    login = login.substr(0, fixedDiffCharPos);
                    if (!difficulty || difficulty != diffValue) {
                        log('warn', logSystem, 'Invalid difficulty value "%s" for login: %s', [diffValue, login]);
                        difficulty = portData.difficulty;
                    } else {
                        noRetarget = true;
                        if (difficulty < config.poolServer.varDiff.minDiff) {
                            difficulty = config.poolServer.varDiff.minDiff;
                        }
                    }
                }
            }

            let addr = login.split(config.poolServer.paymentId.addressSeparator);
            let address = addr[0] || null;
            let paymentId = addr[1] || null;

            if (!address) {
                log('warn', logSystem, 'No address specified for login');
                sendReply('Invalid address used for login');
                return;
            }

            if (paymentId && paymentId.match('^([a-zA-Z0-9]){0,15}$')) {
                if (config.poolServer.paymentId.validation) {
                    process.send({ type: 'banIP', ip: ip + ':' + workerName });
                    log('warn', logSystem, 'Invalid paymentId specified for login');
                } else {
                    log('warn', logSystem, 'Invalid paymentId specified for login');
                }
                sendReply(`Invalid paymentId specified for login, ${portData.ip} banned for ${config.poolServer.banning.time / 60} minutes`);
                return
            }

            /*
            if (!utils.validateMinerAddress(address)) {
                let addressPrefix = utils.getAddressPrefix(address);
                if (!addressPrefix) addressPrefix = 'N/A';

                log('warn', logSystem, 'Invalid address used for login (prefix: %s): %s', [addressPrefix, address]);
                sendReply('Invalid address used for login');
                return;
            }
            */

            let minerId = utils.uid();
            miner = new Miner(minerId, login, pass, ip, port, params.agent, workerName, difficulty, noRetarget, pushMessage);
            connectedMiners[minerId] = miner;
            sendReply(null, {
                id: minerId,
                job: miner.getJob(),
                status: 'OK'
            });

            newConnectedWorker(miner);
            break;
        case 'getjob':
            if (!miner) {
                sendReply('Unauthenticated');
                return;
            }
            miner.heartbeat();
            if (IsBannedIp(ip +':' + miner.workerName)) {
                sendReply('Your IP is banned, worker name is ' + miner.workerName);
                return;
            }
            sendReply(null, miner.getJob());
            break;
        case 'submit':
            if (!miner) {
                sendReply('Unauthenticated');
                return;
            }
            miner.heartbeat();

            if (IsBannedIp(ip +':' + miner.workerName)) {
                sendReply('Your IP is banned, worker name is ' + miner.workerName);
                return;
            }

            let job = miner.validJobs.filter(function (job) {
                return job.id === params.job_id;
            })[0];

            if (!job) {
                sendReply('Invalid job id');
                return;
            }

            if (!params.nonce || !params.result) {
                sendReply('Attack detected');
                let minerText = miner ? (' ' + miner.login + '@' + miner.ip) : '';
                log('warn', logSystem, 'Malformed miner share: ' + JSON.stringify(params) + ' from ' + minerText);
                return;
            }

            if (!noncePattern.test(params.nonce)) {
                let minerText = miner ? (' ' + miner.login + '@' + miner.ip) : '';
                log('warn', logSystem, 'Malformed nonce: ' + JSON.stringify(params) + ' from ' + minerText);
                perIPStats[miner.ip + ':' + miner.workerName] = { validShares: 0, invalidShares: 999999 };
                miner.checkBan(false);
                sendReply('Duplicate share');
                return;
            }

            // Force lowercase for further comparison
            params.nonce = params.nonce.toLowerCase();

            if (!miner.proxy) {
                if (job.submissions.indexOf(params.nonce) !== -1) {
                    let minerText = miner ? (' ' + miner.login + '@' + miner.ip + '@' + miner.workerName) : '';
                    log('info', logSystem, 'Duplicate share: ' + JSON.stringify(params) + ' from ' + minerText);
                    perIPStats[miner.ip + ':' + miner.workerName] = { validShares: 0, invalidShares: 999999 };
                    miner.checkBan(false);
                    sendReply('Duplicate share');
                    return;
                }

                job.submissions.push(params.nonce);
            } else {
                if (!Number.isInteger(params.poolNonce) || !Number.isInteger(params.workerNonce)) {
                    let minerText = miner ? (' ' + miner.login + '@' + miner.ip + '@' + miner.workerName) : '';
                    log('info', logSystem, 'Malformed nonce: ' + JSON.stringify(params) + ' from ' + minerText);
                    perIPStats[miner.ip + ':' + miner.workerName] = { validShares: 0, invalidShares: 999999 };
                    miner.checkBan(false);
                    sendReply('Duplicate share');
                    return;
                }
                let nonce_test = `${params.nonce}_${params.poolNonce}_${params.workerNonce}`;
                if (job.submissions.indexOf(nonce_test) !== -1) {
                    let minerText = miner ? (' ' + miner.login + '@' + miner.ip + '@' + miner.workerName) : '';
                    log('warn', logSystem, 'Duplicate share: ' + JSON.stringify(params) + ' from ' + minerText);
                    perIPStats[miner.ip + ':' + miner.workerName] = { validShares: 0, invalidShares: 999999 };
                    miner.checkBan(false);
                    sendReply('Duplicate share');
                    return;
                }
                job.submissions.push(nonce_test);
            }

            let blockTemplate = currentBlockTemplate.height === job.height ? currentBlockTemplate : validBlockTemplates.filter(function (t) {
                return t.height === job.height;
            })[0];

            if (!blockTemplate) {
                sendReply('Block expired');
                return;
            }

            let shareAccepted = processShare(miner, job, blockTemplate, params);
            miner.checkBan(shareAccepted);

            if (shareTrustEnabled) {
                if (shareAccepted) {
                    miner.trust.probability -= shareTrustStepFloat;
                    if (miner.trust.probability < shareTrustMinFloat)
                        miner.trust.probability = shareTrustMinFloat;
                    miner.trust.penalty--;
                    miner.trust.threshold--;
                }
                else {
                    log('warn', logSystem, 'Share trust broken by %s@%s', [miner.login, miner.ip]);
                    miner.trust.probability = 1;
                    miner.trust.penalty = config.poolServer.shareTrust.penalty;
                }
            }
            //log('warn', logSystem, 'job2:%j', [job]);

            if (!shareAccepted) {
                sendReply('Rejected share: invalid result');//拒绝共享  结果无效
                return;
            }

            let now = Date.now() / 1000 | 0;
            miner.shareTimeRing.append(now - miner.lastShareTime);
            miner.lastShareTime = now;
            //miner.retarget(now);

            sendReply(null, { status: 'OK' });
            break;
        case 'keepalived':
            if (!miner) {
                sendReply('Unauthenticated');
                return;
            }
            miner.heartbeat();
            if (IsBannedIp(ip +':' + miner.workerName)) {
                sendReply('Your IP is banned, worker name is ' + miner.workerName);
                return;
            }
            sendReply(null, { status: 'KEEPALIVED' });
            break;
        default:
            sendReply('Invalid method');
            let minerText = miner ? (' ' + miner.login + '@' + miner.ip) : '';
            log('warn', logSystem, 'Invalid method: %s (%j) from %s', [method, params, minerText]);
            break;
    }
}

/**
 * New connected worker
 **/
function newConnectedWorker(miner) {
    log('info', logSystem, 'Miner connected %s@%s on port', [miner.login, miner.ip, miner.port]);
    if (miner.workerName !== 'undefined') log('info', logSystem, 'Worker Name: %s', [miner.workerName]);
    if (miner.difficulty) log('info', logSystem, 'Miner difficulty fixed to %s', [miner.difficulty]);

    redisClient.sadd(config.coin + ':workers_ip:' + miner.login, miner.ip);//::ffff:127.0.0.1
    redisClient.hincrby(config.coin + ':ports:' + miner.port, 'users', 1);

    redisClient.hincrby(config.coin + ':active_connections', miner.login + '~' + miner.workerName, 1, function (error, connectedWorkers) {
        if (connectedWorkers === 1) {
            notifications.sendToMiner(miner.login, 'workerConnected', {
                'LOGIN': miner.login,
                'MINER': miner.login.substring(0, 7) + '...' + miner.login.substring(miner.login.length - 7),
                'IP': miner.ip.replace('::ffff:', ''),
                'PORT': miner.port,
                'WORKER_NAME': miner.workerName !== 'undefined' ? miner.workerName : ''
            });
        }
    });
}

/**
 * Remove connected worker
 **/
function removeConnectedWorker(miner, reason) {
    redisClient.hincrby(config.coin + ':ports:' + miner.port, 'users', '-1');

    redisClient.hincrby(config.coin + ':active_connections', miner.login + '~' + miner.workerName, -1, function (error, connectedWorkers) {
        if (reason === 'banned') {
            notifications.sendToMiner(miner.login, 'workerBanned', {
                'LOGIN': miner.login,
                'MINER': miner.login.substring(0, 7) + '...' + miner.login.substring(miner.login.length - 7),
                'IP': miner.ip.replace('::ffff:', ''),
                'PORT': miner.port,
                'WORKER_NAME': miner.workerName !== 'undefined' ? miner.workerName : ''
            });
        } else if (!connectedWorkers || connectedWorkers <= 0) {
            notifications.sendToMiner(miner.login, 'workerTimeout', {
                'LOGIN': miner.login,
                'MINER': miner.login.substring(0, 7) + '...' + miner.login.substring(miner.login.length - 7),
                'IP': miner.ip.replace('::ffff:', ''),
                'PORT': miner.port,
                'WORKER_NAME': miner.workerName !== 'undefined' ? miner.workerName : '',
                'LAST_HASH': utils.dateFormat(new Date(miner.lastBeat), 'yyyy-mm-dd HH:MM:ss Z')
            });
        }
    });
}

/**
 * Return if IP has been banned
 * param is ip = ip+':'+workName
 **/
function IsBannedIp(ip) {
    if (!banningEnabled || !bannedIPs[ip]) return false;

    let bannedTime = bannedIPs[ip];
    let bannedTimeAgo = Date.now() - bannedTime;
    let timeLeft = config.poolServer.banning.time * 1000 - bannedTimeAgo;
    if (timeLeft > 0) {
        return true;
    }
    else {
        delete bannedIPs[ip];
        log('info', logSystem, 'Ban dropped for %s', [ip]);
        return false;
    }
}

/**
 * Record miner share data
 **/
function recordShareData(miner, job, shareDiff, blockCandidate, hashHex, shareType, blockTemplate) {
    let dateNow = Date.now();
    let dateNowSeconds = dateNow / 1000 | 0;
    //let diff = 1 << job.difficulty;
    let diff = Math.pow(2, job.difficulty);

    let updateScore;
    // Weighting older shares lower than newer ones to prevent pool hopping
    if (slushMiningEnabled) {
        // We need to do this via an eval script because we need fetching the last block time and
        // calculating the score to run in a single transaction (otherwise we could have a race
        // condition where a block gets discovered between the time we look up lastBlockFound and
        // insert the score, which would give the miner an erroneously huge proportion on the new block)
        updateScore = ['eval', `
            local age = (ARGV[3] - redis.call('hget', KEYS[2], 'lastBlockFound')) / 1000
            local score = string.format('%.17g', ARGV[2] * math.exp(age / ARGV[4]))
            redis.call('hincrbyfloat', KEYS[1], ARGV[1], score)
            return {score, tostring(age)}
            `,
            2 /*keys*/, config.coin + ':scores:roundCurrent', config.coin + ':stats',
            /* args */ miner.login, diff, Date.now(), config.poolServer.slushMining.weight];
    }
    else {
        job.score = diff;
        updateScore = ['hincrbyfloat', config.coin + ':scores:roundCurrent', miner.login, job.score]
    }

    let redisCommands = [
        updateScore,
        ['hincrby', config.coin + ':shares_actual:roundCurrent', miner.login, diff],
        ['zadd', config.coin + ':hashrate', dateNowSeconds, [diff, miner.login, dateNow].join(':')],
        ['hincrby', config.coin + ':workers:' + miner.login, 'hashes', diff],
        ['hset', config.coin + ':workers:' + miner.login, 'lastShare', dateNowSeconds],
        ['expire', config.coin + ':workers:' + miner.login, (86400 * cleanupInterval)],
        ['expire', config.coin + ':payments:' + miner.login, (86400 * cleanupInterval)]
    ];

    //expire:设置 key 的过期时间，key 过期后将不再可用。单位以秒计
    if (miner.workerName) {
        redisCommands.push(['zadd', config.coin + ':hashrate', dateNowSeconds, [diff, miner.login + '~' + miner.workerName, dateNow].join(':')]);
        redisCommands.push(['hincrby', config.coin + ':unique_workers:' + miner.login + '~' + miner.workerName, 'hashes', diff]);
        redisCommands.push(['hset', config.coin + ':unique_workers:' + miner.login + '~' + miner.workerName, 'lastShare', dateNowSeconds]);
        redisCommands.push(['expire', config.coin + ':unique_workers:' + miner.login + '~' + miner.workerName, (86400 * cleanupInterval)]);
    }

    //log('error', logSystem, 'blockCandidate:%s', [blockCandidate]);

    if (blockCandidate) {
        redisCommands.push(['hset', config.coin + ':stats', 'lastBlockFound', Date.now()]);
        redisCommands.push(['rename', config.coin + ':scores:roundCurrent', config.coin + ':scores:round' + job.height]);
        redisCommands.push(['rename', config.coin + ':shares_actual:roundCurrent', config.coin + ':shares_actual:round' + job.height]);
        redisCommands.push(['hgetall', config.coin + ':scores:round' + job.height]);
        redisCommands.push(['hgetall', config.coin + ':shares_actual:round' + job.height]);
        redisCommands.push(['lpush', config.coin + ':blockrecord:' + miner.login + '~' + miner.workerName, dateNowSeconds + ':' + job.height + ':' + job.difficulty])
    }

    redisClient.multi(redisCommands).exec(function (err, replies) {
        if (err) {
            log('error', logSystem, 'Failed to insert share data into database %j \n %j', [err, redisCommands]);
            return;
        }

        //log('error', logSystem, 'pool_replies:%j', [replies]);

        if (slushMiningEnabled) {
            job.score = parseFloat(replies[0][0]);
            let age = parseFloat(replies[0][1]);
            //log('info', logSystem, 'Submitted score ' + job.score + ' for difficulty ' + job.difficulty + ' and round age ' + age + 's');
        }

        if (blockCandidate) {
            let workerScores = replies[replies.length - 2];
            let workerShares = replies[replies.length - 1];
            let totalScore = Object.keys(workerScores).reduce(function (p, c) {
                return p + parseFloat(workerScores[c])
            }, 0);
            let totalShares = Object.keys(workerShares).reduce(function (p, c) {
                return p + parseInt(workerShares[c])
            }, 0);

            redisClient.zadd(config.coin + ':blocks:candidates', job.height, [
                hashHex,
                (Date.now() / 1000 | 0).toString(),
                blockTemplate.difficulty,
                totalShares,
                totalScore
            ].join(':'), function (err, result) {
                if (err) {
                    log('error', logSystem, 'Failed inserting block candidate %s \n %j', [hashHex, err]);
                }
            });

            notifications.sendToAll('blockFound', {
                'HEIGHT': job.height,
                'HASH': hashHex,
                'DIFFICULTY': blockTemplate.difficulty,
                'SHARES': totalShares,
                'MINER': miner.login.substring(0, 7) + '...' + miner.login.substring(miner.login.length - 7)
            });
        }

    });

    //log('info', logSystem, 'Accepted %s share at difficulty %d/%d from %s@%s', [shareType, job.difficulty, shareDiff, miner.login, miner.ip]);
}

/**
 * Process miner share data
 **/
function processShare(miner, job, blockTemplate, params) {
    let nonce = params.nonce;
    var nonce_val0 = parseInt(nonce.substring(0, 8), 16);
    //var nonce_val1 = parseInt(nonce.substring(8, 16), 16);
    var time_val = parseInt(params.time, 16);
    let resultHash = params.result;
    let template = Buffer.alloc(blockTemplate.buffer.length);

    //log("warn", logSystem, 'blockTemplate:%j', [blockTemplate]);

    if (!miner.proxy) {
        blockTemplate.buffer.copy(template);
        template.writeUInt32BE(time_val, 4);
        template.writeUInt32BE(nonce_val0, 109);
        template.writeUInt32BE(job.extraNonce, blockTemplate.reserveOffset);
    } else {
        blockTemplate.buffer.copy(template);
        template.writeUInt32BE(job.extraNonce, blockTemplate.reserveOffset);
        template.writeUInt32BE(params.poolNonce, job.clientPoolLocation);
        template.writeUInt32BE(params.workerNonce, job.clientNonceLocation);
    }

    let tmpTime = template.readUIntLE(4,4);
    let shareBuffer = template;
    let hash;
    let shareType;

    params.time = tmpTime;
    if(params.time < job.timestamp){
        log('warn',logSystem,'workerName:%j,params.time:%d < job.timestamp:%d',[miner.workerName,params.time,job.timestamp]);
        return false;
    }

    if (shareTrustEnabled && miner.trust.threshold <= 0 && miner.trust.penalty <= 0 && Math.random() > miner.trust.probability) {
        hash = Buffer.from(resultHash, 'hex');
        shareType = 'trusted';
    }
    else {
        hash = cryptoNight(shareBuffer, cnVariant);
        shareType = 'valid';
    }

    if (hash.toString('hex') !== resultHash) {
        log('warn', logSystem, 'Bad hash from miner %s@%s@%s', [miner.login, miner.workerName,miner.ip]);
        return false;
    }

    let hashArray = hash.toByteArray().reverse();
    let hashNum = bignum.fromBuffer(Buffer.from(hashArray));
    let hashDiff = diff1.div(hashNum);

    let tpmDiff = Math.pow(2, job.difficulty);
    let nodeDiff = Math.pow(2, blockTemplate.difficulty);

    //log('info', logSystem, 'tpmDiff:%d,nodeDiff:%d,hashDiff:%d', [tpmDiff, nodeDiff, hashDiff]);
    //log('info', logSystem, 'height:%d,job.difficulty:%d,block.difficulty:%d', [blockTemplate.height, job.difficulty, blockTemplate.difficulty]);

    //log('info',logSystem,'job:%j',[job]);
    //log('info',logSystem,'upj:%j',[params]);    
    
    if (hashDiff.ge(tpmDiff)) {
        if (hashDiff.ge(nodeDiff)) {
            apiInterfaces.rpcDaemon('submitwork', {
                spent: config.poolServer.spentAddress.toString(),
                privkey: config.poolServer.privkey.toString(),
                data: shareBuffer.toString('hex')
            }, function (err, result) {
                if (err) {
                    if (err.toString().length > 10 && err.code != -6 && err.code != -4){                        
                        log(
                            'error',logSystem,
                            'Submitting error. height:%d,miner:%s@%s@%s,shareType:%s,%j,diff:%j,timestamp:%d',
                            [job.height, miner.login, miner.workerName, miner.ip, shareType, err, job.difficulty, tmpTime]
                        );
                        recordShareData(miner, job, hashDiff.toString(), false, null, shareType);
                    }else{
                        log(
                            'error',logSystem,
                            'Submitting error. height:%d,miner:%s@%s@%s,shareType:%s,%j,diff:%j,timestamp:%d',
                            [job.height, miner.login, miner.workerName, miner.ip, shareType, err, job.difficulty, tmpTime]
                        );
                    }                    
                    return false;
                }

                let blockFastHash = result.toString('hex');
                log(
                    'info', logSystem, 
                    'Block %s found. height:%d,miner:%s@%s@%s,submit result:%j,shareType:%j,diff:%j,timestamp:%d', 
                    [blockFastHash.substr(8, 14), job.height, miner.login, miner.workerName, miner.ip, result, shareType, job.difficulty, tmpTime]
                );
                recordShareData(miner, job, hashDiff.toString(), true, blockFastHash, shareType, blockTemplate);
                jobRefresh();
            });
        } else {
            log('info', logSystem, 'Submitting block. height:%d,miner:%s@%s@%s,shareType:%s,diff:%j,timestamp:%d', 
            [job.height, miner.login, miner.workerName, miner.ip, shareType, job.difficulty, tmpTime]);
            recordShareData(miner, job, hashDiff.toString(), false, null, shareType);
        }
    } else {
        log('warn', logSystem, 'Rejected low difficulty share.miner:%s@%s@%s,diff:%j,timestamp:%d', 
        [miner.login, miner.workerName, miner.ip, job.difficulty,tmpTime]);
        return false;
    }
    return true;
}

/**
 * Start pool server on TCP ports
 **/
let httpResponse = ' 200 OK\nContent-Type: text/plain\nContent-Length: 20\n\nMining server online';

function startPoolServerTcp(callback) {
    log('info', logSystem, 'Clear values for connected workers in database.');
    redisClient.del(config.coin + ':active_connections');

    async.each(config.poolServer.ports, function (portData, cback) {
        let handleMessage = function (socket, jsonData, pushMessage) {
            if (!jsonData.id) {
                log('warn', logSystem, 'Miner RPC request missing RPC id');
                return;
            }
            else if (!jsonData.method) {
                log('warn', logSystem, 'Miner RPC request missing RPC method');
                return;
            }
            else if (!jsonData.params) {
                log('warn', logSystem, 'Miner RPC request missing RPC params');
                return;
            }

            let sendReply = function (error, result) {
                if (!socket.writable) return;
                let sendData = JSON.stringify({
                    id: jsonData.id,
                    jsonrpc: "2.0",
                    error: error ? { code: -1, message: error } : null,
                    result: result
                }) + "\n";
                socket.write(sendData);
            };

            handleMinerMethod(jsonData.method, jsonData.params, socket.remoteAddress, portData, sendReply, pushMessage);
        };

        let socketResponder = function (socket) {
            socket.setKeepAlive(true);
            socket.setEncoding('utf8');

            let dataBuffer = '';

            let pushMessage = function (method, params) {
                if (!socket.writable) return;
                let sendData = JSON.stringify({
                    jsonrpc: "2.0",
                    method: method,
                    params: params
                }) + "\n";
                socket.write(sendData);
            };

            socket.on('data', function (d) {
                dataBuffer += d;
                if (Buffer.byteLength(dataBuffer, 'utf8') > 10240) { //10KB
                    dataBuffer = null;
                    log('warn', logSystem, 'Socket flooding detected and prevented from %s', [socket.remoteAddress]);
                    socket.destroy();
                    return;
                }
                if (dataBuffer.indexOf('\n') !== -1) {
                    let messages = dataBuffer.split('\n');
                    let incomplete = dataBuffer.slice(-1) === '\n' ? '' : messages.pop();
                    for (let i = 0; i < messages.length; i++) {
                        let message = messages[i];
                        if (message.trim() === '') continue;
                        let jsonData;
                        try {
                            jsonData = JSON.parse(message);
                        }
                        catch (e) {
                            if (message.indexOf('GET /') === 0) {
                                if (message.indexOf('HTTP/1.1') !== -1) {
                                    socket.end('HTTP/1.1' + httpResponse);
                                    break;
                                }
                                else if (message.indexOf('HTTP/1.0') !== -1) {
                                    socket.end('HTTP/1.0' + httpResponse);
                                    break;
                                }
                            }

                            log('warn', logSystem, 'Malformed message from %s: %s', [socket.remoteAddress, message]);
                            socket.destroy();
                            break;
                        }
                        try {
                            handleMessage(socket, jsonData, pushMessage);
                        } catch (e) {
                            log('warn', logSystem, 'Malformed message from ' + socket.remoteAddress + ' generated an exception. Message: ' + message);
                            if (e.message) log('warn', logSystem, 'Exception: ' + e.message);
                        }
                    }
                    dataBuffer = incomplete;
                }
            }).on('error', function (err) {
                if (err.code !== 'ECONNRESET')
                    log('warn', logSystem, 'Socket error from %s %j', [socket.remoteAddress, err]);
            }).on('close', function () {
                pushMessage = function () { };
            });
        };

        if (portData.ssl) {
            if (!config.poolServer.sslCert) {
                log('error', logSystem, 'Could not start server listening on port %d (SSL): SSL certificate not configured', [portData.port]);
                cback(true);
            } else if (!config.poolServer.sslKey) {
                log('error', logSystem, 'Could not start server listening on port %d (SSL): SSL key not configured', [portData.port]);
                cback(true);
            } else if (!config.poolServer.sslCA) {
                log('error', logSystem, 'Could not start server listening on port %d (SSL): SSL certificate authority not configured', [portData.port]);
                cback(true);
            } else if (!fs.existsSync(config.poolServer.sslCert)) {
                log('error', logSystem, 'Could not start server listening on port %d (SSL): SSL certificate file not found (configuration error)', [portData.port]);
                cback(true);
            } else if (!fs.existsSync(config.poolServer.sslKey)) {
                log('error', logSystem, 'Could not start server listening on port %d (SSL): SSL key file not found (configuration error)', [portData.port]);
                cback(true);
            } else if (!fs.existsSync(config.poolServer.sslCA)) {
                log('error', logSystem, 'Could not start server listening on port %d (SSL): SSL certificate authority file not found (configuration error)', [portData.port]);
                cback(true);
            } else {
                let options = {
                    key: fs.readFileSync(config.poolServer.sslKey),
                    cert: fs.readFileSync(config.poolServer.sslCert),
                    ca: fs.readFileSync(config.poolServer.sslCA)
                };
                tls.createServer(options, socketResponder).listen(portData.port, function (error, result) {
                    if (error) {
                        log('error', logSystem, 'Could not start server listening on port %d (SSL), error: $j', [portData.port, error]);
                        cback(true);
                        return;
                    }

                    log('info', logSystem, 'Clear values for SSL port %d in database.', [portData.port]);
                    redisClient.del(config.coin + ':ports:' + portData.port);
                    redisClient.hset(config.coin + ':ports:' + portData.port, 'port', portData.port);

                    log('info', logSystem, 'Started server listening on port %d (SSL)', [portData.port]);
                    cback();
                });
            }
        }
        else {
            net.createServer(socketResponder).listen(portData.port, function (error, result) {
                if (error) {
                    log('error', logSystem, 'Could not start server listening on port %d, error: $j', [portData.port, error]);
                    cback(true);
                    return;
                }

                log('info', logSystem, 'Clear values for port %d in database.', [portData.port]);
                redisClient.del(config.coin + ':ports:' + portData.port);
                redisClient.hset(config.coin + ':ports:' + portData.port, 'port', portData.port);

                log('info', logSystem, 'Started server listening on port %d', [portData.port]);
                cback();
            });
        }
    }, function (err) {
        if (err)
            callback(false);
        else
            callback(true);
    });
}

function strToHex(str) {
    let re = parseInt(str, 2).toString(16);
    if (Number(re) >= 0 && Number(re) <= 9) {
        re = '0' + re;
    }
    return re;
}

function pad(str, n) {
    var len = str.length;
    while (len < n) {
        str = "0" + str;
        len++;
    }
    return str;
}

/**
 * Initialize pool server
 **/

(function init() {
    jobRefresh(true, function (sucessful) { });
})();
