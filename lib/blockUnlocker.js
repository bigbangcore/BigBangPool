/**
 * Cryptonote Node.JS Pool
 * https://github.com/dvandal/cryptonote-nodejs-pool
 *
 * Block unlocker
 **/

// Load required modules
let async = require('async');

let apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet, config.api);
let notifications = require('./notifications.js');
let utils = require('./utils.js');

let slushMiningEnabled = config.poolServer.slushMining && config.poolServer.slushMining.enabled;

// Initialize log system
let logSystem = 'unlocker';
require('./exceptionWriter.js')(logSystem);

/**
 * Run block unlocker
 **/

log('info', logSystem, 'Started');

function runInterval() {
    // 按顺序依次执行多个函数。每一个函数产生的值，都将传给下一个函数。如果中途出错，后面的函数将不会被执行。
    // 错误信息以及之前产生的结果，将传给waterfall最终的callback
    async.waterfall([
        // Get all block candidates in redis
        function (callback) {
            redisClient.zrange(config.coin + ':blocks:candidates', 0, -1, 'WITHSCORES', function (error, results) {
                if (error) {
                    log('error', logSystem, 'Error trying to get pending blocks from redis %j', [error]);
                    callback(true);
                    return;
                }
                if (results.length === 0) {
                    log('info', logSystem, 'No blocks candidates in redis');
                    callback(true);
                    return;
                }
                else {
                    log('info', logSystem, '%d blocks candidates in redis', [results.length]);
                }

                let blocks = [];

                for (let i = 0; i < results.length; i += 2) {
                    let parts = results[i].split(':');
                    let s = parts.length >= 5 ? parts[4] : parts[3];
                    blocks.push({
                        serialized: results[i],
                        height: parseInt(results[i + 1]),
                        hash: parts[0],
                        time: parts[1],
                        difficulty: parts[2],
                        shares: parts[3],
                        score: parts.length >= 5 ? parts[4] : parts[3]
                    });
                }
                //在这里通过回调函数把blocks传给下一个函数
                //记得一定要加上null，才能调用数组的下一个函数，否则，会直接调用最终的回调函数，然后结束函数，则后面的函数将不再执行
                callback(null, blocks);
            });
        },

        // Check if blocks are orphaned,检查块是否孤立
        function (blocks, callback) {
            let currentHeight = blocks[blocks.length - 1].height;
            // 过滤器
            async.filter(blocks, function (block, mapCback) {
                let daemonType = config.daemonType ? config.daemonType.toLowerCase() : "default";
                let blockHeight = (daemonType === "forknote" && config.blockUnlocker.fixBlockHeightRPC) ? block.height + 1 : block.height;
                //let rpcmethod = config.blockUnlocker.useFirstVout ? 'getblockhash' : 'getblockheaderbyheight';
                apiInterfaces.rpcDaemon('getblockhash', { height: blockHeight }, function (error, bhash) {
                    if (error) {
                        log('error', logSystem, 'Error with getblockhash: %j', [error]);
                        block.unlocked = false;
                        mapCback();
                        return;
                    }
                    let rpcmethod = 'getblockdetail';
                    apiInterfaces.rpcDaemon(rpcmethod, { block: bhash.toString() }, function (error, result) {
                        if (result.length < 10) {
                            log('info', logSystem, '%s %s returns null!', [rpcmethod, bhash.toString()]);
                            block.unlocked = false;
                            mapCback();
                            return;
                        }

                        let blockHeader = result;
                        block.orphaned = blockHeader.hash === block.hash ? 0 : 1;

                        if (currentHeight - blockHeader.height >= config.blockUnlocker.depth) {
                            block.unlocked = true;
                        }
                        else {
                            block.unlocked = false;
                        }

                        //block.unlocked = blockHeader.txmint.lockuntil >= config.blockUnlocker.depth;
                        //block.unlocked = true;
                        block.reward = blockHeader.txmint.amount;

                        if (config.blockUnlocker.networkFee) {
                            let networkFeePercent = config.blockUnlocker.networkFee / 100;
                            block.reward = block.reward - (block.reward * networkFeePercent);
                        }
                        mapCback(block.unlocked);
                    });
                });
            }, function (unlockedBlocks) {
                if (unlockedBlocks.length === 0) {
                    log('info', logSystem, 'No pending blocks are unlocked yet (%d pending)', [blocks.length]);
                    callback(true);
                    return;
                }
                callback(null, unlockedBlocks);
            });
        },

        // Get worker shares for each unlocked block
        function (unlockedBlocks, callback) {
            //log('error', logSystem, 'xblocks:%j', [blocks]);
            let redisCommands = unlockedBlocks.map(function (block) {
                return ['hgetall', config.coin + ':scores:round' + block.height];
            });

            redisClient.multi(redisCommands).exec(function (error, replies) {
                if (error) {
                    log('error', logSystem, 'Error with getting round shares from redis %j', [error]);
                    callback(true);
                    return;
                }
                //log('error', logSystem, 'xxreplies:%j', [replies]);
                for (let i = 0; i < replies.length; i++) {
                    let workerScores = replies[i];
                    unlockedBlocks[i].workerScores = workerScores;
                }
                //log('error', logSystem, 'xxblocks:%j', [blocks]);
                callback(null, unlockedBlocks);
            });
        },

        // Handle orphaned blocks,处理孤立的区块，啥意思？
        function (unlockedBlocks, callback) {
            let orphanCommands = [];

            unlockedBlocks.forEach(function (block) {
                if (!block.orphaned) return;

                orphanCommands.push(['del', config.coin + ':scores:round' + block.height]);
                orphanCommands.push(['del', config.coin + ':shares_actual:round' + block.height]);

                // zrem,移除有序集合中的一个或多个成员
                orphanCommands.push(['zrem', config.coin + ':blocks:candidates', block.serialized]);
                orphanCommands.push(['zadd', config.coin + ':blocks:matured', block.height, [
                    block.hash,
                    block.time,
                    block.difficulty,
                    block.shares,
                    block.orphaned
                ].join(':')]);

                if (block.workerScores && !slushMiningEnabled) {
                    let workerScores = block.workerScores;
                    Object.keys(workerScores).forEach(function (worker) {
                        orphanCommands.push(['hincrby', config.coin + ':scores:roundCurrent', worker, workerScores[worker]]);
                    });
                }

                notifications.sendToAll('blockOrphaned', {
                    'HEIGHT': block.height,
                    'BLOCKTIME': utils.dateFormat(new Date(parseInt(block.time) * 1000), 'yyyy-mm-dd HH:MM:ss Z'),
                    'HASH': block.hash,
                    'DIFFICULTY': block.difficulty,
                    'SHARES': block.shares,
                    'EFFORT': utils.toFixed(block.shares / Math.pow(2, block.difficulty) * 100) + '%'
                });
            });

            if (orphanCommands.length > 0) {
                redisClient.multi(orphanCommands).exec(function (error, replies) {
                    if (error) {
                        log('error', logSystem, 'Error with cleaning up data in redis for orphan block(s) %j', [error]);
                        callback(true);
                        return;
                    }
                    callback(null, unlockedBlocks);
                });
            }
            else {
                callback(null, unlockedBlocks);
            }
        },

        // Handle unlocked blocks
        function (unlockedBlocks, callback) {
            let unlockedBlocksCommands = [];
            let payments = {};
            let totalBlocksUnlocked = 0;
            unlockedBlocks.forEach(function (block) {
                if (block.orphaned) return;
                totalBlocksUnlocked++;

                unlockedBlocksCommands.push(['del', config.coin + ':scores:round' + block.height]);
                unlockedBlocksCommands.push(['del', config.coin + ':shares_actual:round' + block.height]);
                unlockedBlocksCommands.push(['zrem', config.coin + ':blocks:candidates', block.serialized]);
                unlockedBlocksCommands.push(['zadd', config.coin + ':blocks:matured', block.height, [
                    block.hash,
                    block.time,
                    block.difficulty,
                    block.shares,
                    block.orphaned,
                    block.reward
                ].join(':')]);

                let feePercent = config.blockUnlocker.poolFee / 100;//0.8%
                let wallet = config.blockUnlocker.devDonationAddr;//1z6x6rjzax4wag3bgz4123j9fq3pt8pyvtf32qtxrx7fzah7g2xmej6kb
                let percent = donations[wallet] ? donations[wallet] / 100 : 0;//0.2%
                //log('warn', logSystem, 'donations:%j', [donations]);
                feePercent += percent;//1%
                //payments[wallet] = Math.round(block.reward * percent);
                //let reward = Math.round(block.reward - (block.reward * feePercent));
                //let fee = Math.round(block.reward * feePercent);
                payments[wallet] = utils.toFixed(block.reward * percent);
                let reward = utils.toFixed(block.reward - (block.reward * feePercent));
                let fee = utils.toFixed(block.reward * feePercent);

                log('info', logSystem, 'Unlocked %d block with reward %d and donation fee %d. Miners reward: %d', [block.height, block.reward, fee, reward]);

                if (block.workerScores) {
                    let totalScore = parseFloat(block.score);

                    log('info', logSystem, 'totalScore:%j', [totalScore]);
                    log('info', logSystem, 'block.workerScores:%j', [block.workerScores]);

                    Object.keys(block.workerScores).forEach(function (worker) {
                        //TODO:在这里按照算力分币，检查是否正确
                        let percent = block.workerScores[worker] / totalScore;
                        //let workerReward = Math.round(reward * percent);
                        let workerReward = utils.toFixed(reward * percent);

                        log('info', logSystem, 'worker:%j,workerReward:%j,diff:%j', [worker, workerReward, block.difficulty]);
                        log('info', logSystem, 'block.workerScores[worker]:%j--worker:%j', [block.workerScores[worker], worker]);

                        payments[worker] = Number(payments[worker] || 0) + Number(workerReward);
                        log('info', logSystem, 'Block %d payment to %s for %d%% of total block score: %j', [block.height, worker, percent * 100, payments[worker]]);
                    });
                }

                notifications.sendToAll('blockUnlocked', {
                    'HEIGHT': block.height,
                    'BLOCKTIME': utils.dateFormat(new Date(parseInt(block.time) * 1000), 'yyyy-mm-dd HH:MM:ss Z'),
                    'HASH': block.hash,
                    'REWARD': utils.getReadableCoins(block.reward),
                    'DIFFICULTY': block.difficulty,
                    'SHARES': block.shares,
                    'EFFORT': utils.toFixed(block.shares / (Math.pow(2, block.difficulty)) * 100) + '%'
                });
            });

            //log('warn', logSystem, 'payments:%j', [payments]);

            for (let worker in payments) {
                let amount = parseInt(payments[worker]);
                if (amount <= 0) {
                    delete payments[worker];
                    continue;
                }
                // 为哈希表中的字段值加上指定增量值
                //unlockedBlocksCommands.push(['hincrby', config.coin + ':workers:' + worker, 'balance', amount]);
                unlockedBlocksCommands.push(['HINCRBYFLOAT', config.coin + ':workers:' + worker, 'balance', payments[worker]]);
            }

            if (unlockedBlocksCommands.length === 0) {
                log('info', logSystem, 'No unlocked blocks yet (%d pending)', [unlockedBlocks.length]);
                callback(true);
                return;
            }

            redisClient.multi(unlockedBlocksCommands).exec(function (error, replies) {
                if (error) {
                    log('error', logSystem, 'Error with unlocking blocks %j', [error]);
                    callback(true);
                    return;
                }
                log('info', logSystem, 'Unlocked %d blocks and update balances for %d workers', [totalBlocksUnlocked, Object.keys(payments).length]);
                callback(null);
            });
        }
    ], function (error, result) {
        setTimeout(runInterval, config.blockUnlocker.interval * 1000);
    })
}

runInterval();

