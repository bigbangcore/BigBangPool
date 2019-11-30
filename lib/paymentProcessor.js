/**
 * Cryptonote Node.JS Pool
 * https://github.com/dvandal/cryptonote-nodejs-pool
 *
 * Payments processor
 **/

// Load required modules
let fs = require('fs');
let async = require('async');

let apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet, config.api);
let notifications = require('./notifications.js');
let utils = require('./utils.js');

// Initialize log system
let logSystem = 'payments';
require('./exceptionWriter.js')(logSystem);

/**
 * Run payments processor
 **/

log('info', logSystem, 'Started');

if (!config.poolServer.paymentId) config.poolServer.paymentId = {};
if (!config.poolServer.paymentId.addressSeparator) config.poolServer.paymentId.addressSeparator = "+";
if (!config.payments.priority) config.payments.priority = 0;

function runInterval() {
    async.waterfall([
        // Get worker keys
        function (callback) {
            /**
                1) "Bigbang:workers:1s8vrd1fasvr3h1w0sh4bxjwhzz76hm5a702gy3khzcn70n4dtsf94sr6"
                2) "Bigbang:workers:1qdq3txbf0mep3tke3v4kpkm6zeyxzs0901e760r8xade696yffks2q6g"
                3) "Bigbang:workers:1n403j5amwf9ran5qrfmdr58zm2ztxywsbh6waqgjft9nrz554p1n783c"
                4) "Bigbang:workers:1jtacnnqtrmm6g7n0tphyjrqp0rkapvr49m2qwhx8kt6x8c83mh2c19ds"
             */
            redisClient.keys(config.coin + ':workers:*', function (error, result) {
                if (error) {
                    log('error', logSystem, 'Error trying to get worker balances from redis %j', [error]);
                    callback(true);
                    return;
                }
                log('info', logSystem, 'workers:%j', [result]);
                callback(null, result);
            });
        },

        // Get worker balances
        function (keys, callback) {
            let redisCommands = keys.map(function (k) {
                return ['hget', k, 'balance'];
            });
            
            redisClient.multi(redisCommands).exec(function (error, replies) {
                if (error) {
                    log('error', logSystem, 'Error with getting balances from redis %j', [error]);
                    callback(true);
                    return;
                }

                log('info', logSystem, 'replies1:%j', [replies]);

                let balances = {};
                for (let i = 0; i < replies.length; i++) {
                    let parts = keys[i].split(':');
                    let workerId = parts[parts.length - 1];
                    log('info', logSystem, 'workerId:%j', [workerId]);

                    balances[workerId] = replies[i] || 0;                   
                }
                log('info', logSystem, 'balances:%j', [balances]);
                callback(null, keys, balances);
            });
        },

        // Get worker minimum payout
        function (keys, balances, callback) {
            let redisCommands = keys.map(function (k) {
                return ['hget', k, 'minPayoutLevel'];
            });
            redisClient.multi(redisCommands).exec(function (error, replies) {
                if (error) {
                    log('error', logSystem, 'Error with getting minimum payout from redis %j', [error]);
                    callback(true);
                    return;
                }

                log('info', logSystem, 'replies2:%j', [replies]);

                let minPayoutLevel = {};
                for (let i = 0; i < replies.length; i++) {
                    let parts = keys[i].split(':');
                    let workerId = parts[parts.length - 1];

                    log('info', logSystem, 'workerId:%j', [workerId]);

                    let minLevel = config.payments.minPayment;
                    let maxLevel = config.payments.maxPayment;
                    let defaultLevel = minLevel;

                    let payoutLevel = parseInt(replies[i]) || minLevel;
                    if (payoutLevel < minLevel) payoutLevel = minLevel;
                    if (maxLevel && payoutLevel > maxLevel) payoutLevel = maxLevel;
                    minPayoutLevel[workerId] = payoutLevel;

                    if (payoutLevel !== defaultLevel) {
                        log('info', logSystem, 'Using payout level of %s for %s (default: %s)', [utils.getReadableCoins(minPayoutLevel[workerId]), workerId, utils.getReadableCoins(defaultLevel)]);
                    }
                }
                //log('warn', logSystem, 'minPayoutLevel:%j', [minPayoutLevel]);
                callback(null, balances, minPayoutLevel);
            });
        },

        // Filter workers under balance threshold for payment
        function (balances, minPayoutLevel, callback) {
            let payments = {};

            for (let worker in balances) {
                let balance = balances[worker];
                log('info', logSystem, 'balanceB:%j',[balance]);
                log('info', logSystem, 'workerB:%j',[worker]);
                log('info', logSystem, 'minPayoutLevel[worker]:%j',[minPayoutLevel[worker]]);
                if (balance >= minPayoutLevel[worker]) {
                    let remainder = balance % config.payments.denomination;
                    let payout = balance - remainder;

                    if (config.payments.dynamicTransferFee && config.payments.minerPayFee) {
                        payout -= config.payments.transferFee;
                    }
                    if (payout < 0) continue;
                    payments[worker] = payout;
                    log('info', logSystem, 'payout:%j',[payout]);
                }
            }

            log('info', logSystem, 'payments:%j',[payments]);

            if (Object.keys(payments).length === 0) {
                log('info', logSystem, 'No workers\' balances reached the minimum payment threshold');
                callback(true);
                return;
            }

            let transferCommands = [];
            let addresses = 0;
            let commandAmount = 0;
            let commandIndex = 0;            

            for (let worker in payments) {
                log('info', logSystem, 'workerC:%j',[worker]);
                let amount = payments[worker];
                log('info', logSystem, 'amountA:%j',[amount]);
                log('info', logSystem, 'commandAmountA:%j',[commandAmount]);
                if (config.payments.maxTransactionAmount && (amount + commandAmount) > config.payments.maxTransactionAmount) {
                    amount = config.payments.maxTransactionAmount - commandAmount;
                    log('info', logSystem, 'amountB:%j',[amount]);
                }                

                let address = worker;
                let payment_id = null;

                let with_payment_id = false;

                let addr = address.split(config.poolServer.paymentId.addressSeparator);

                log('info', logSystem, 'addrA:%j', [addr]);
                //if ((addr.length === 1 && utils.isIntegratedAddress(address)) || addr.length >= 2){
                with_payment_id = true;
                if (addr.length >= 1) {
                    address = addr[0];
                    log('info', logSystem, 'addressA:%j', [address]);
                    payment_id = addr[0];
                    log('info', logSystem, 'payment_id1:%j', [payment_id]);
                    //payment_id = payment_id.replace(/[^A-Za-z0-9]/g, '');
                    //log('info', logSystem, 'payment_id2:%j', [payment_id]);
                    if (payment_id.length !== 16 && (address.length < 50 || address.length > 64)) {
                        with_payment_id = false;
                        payment_id = null;
                    }
                }
                log('info', logSystem, 'addressesA:%j', [addresses]);
                if (addresses > 0) {
                    commandIndex++;
                    addresses = 0;
                    commandAmount = 0;
                }
                log('info', logSystem, 'commandIndex:%j', [commandIndex]);

                if (config.poolServer.fixedDiff && config.poolServer.fixedDiff.enabled) {
                    let addr = address.split(config.poolServer.fixedDiff.addressSeparator);
                    log('info', logSystem, 'addrB:%j', [addr]);
                    if (addr.length >= 1) address = addr[0];
                }

                log('info', logSystem, 'addressB:%j', [address]);

                if (!transferCommands[commandIndex]) {
                    log('info', logSystem, 'transferCommandscommandIndex:%j', [commandIndex]);
                    transferCommands[commandIndex] = {
                        redis: [],
                        amount: 0,
                        destinations: [],
                        rpc: {
                            from: config.poolServer.templateAddress,
                            to: address,
                            amount: 0,
                            txfee: config.payments.transferFee/*,
                            //data: 'msg:rewards from pool'*/
                        }
                    };
                    log('info', logSystem, 'transferCommands[%d]:%j', [commandIndex,transferCommands[commandIndex]]);
                }

                //给transferCommands的各个参数设置值
                transferCommands[commandIndex].destinations.push({ amount: amount, address: address });
                if (payment_id) transferCommands[commandIndex].payment_id = payment_id;

                transferCommands[commandIndex].redis.push(['HINCRBYFLOAT', config.coin + ':workers:' + worker, 'balance', -amount]);
                if (config.payments.dynamicTransferFee && config.payments.minerPayFee) {
                    transferCommands[commandIndex].redis.push(['HINCRBYFLOAT', config.coin + ':workers:' + worker, 'balance', -config.payments.transferFee]);
                }
                //transferCommands[commandIndex].redis.push(['HINCRBYFLOAT', config.coin + ':workers:' + worker, 'balance', -1]);
                transferCommands[commandIndex].redis.push(['HINCRBYFLOAT', config.coin + ':workers:' + worker, 'paid', amount]);
                transferCommands[commandIndex].amount += amount;
                transferCommands[commandIndex].rpc.amount += amount;

                addresses++;
                commandAmount += amount;

                log('info', logSystem, 'addresses++:%j', [addresses]);
                log('info', logSystem, 'commandAmount+=:%j', [commandAmount]);
                log('info', logSystem, 'amount:%j', [amount]);

                if (config.payments.dynamicTransferFee) {
                    transferCommands[commandIndex].rpc.txfee = config.payments.transferFee * addresses;
                }

                if (addresses >= config.payments.maxAddresses || (config.payments.maxTransactionAmount && commandAmount >= config.payments.maxTransactionAmount) || with_payment_id) {
                    commandIndex++;
                    log('info', logSystem, 'commandIndex++:%j', [commandIndex]);
                    addresses = 0;
                    commandAmount = 0;
                }
            }

            let timeOffset = 0;
            let notify_miners = [];

            let daemonType = config.daemonType ? config.daemonType.toLowerCase() : "default";

            log('info', logSystem, 'transferCommands: %j', [transferCommands]);

            async.filter(transferCommands, function (transferCmd, cback) {
                log('info', logSystem, 'transferCmd: %j', [transferCmd]);
                let rpcRequest = transferCmd.rpc;

                log('info', logSystem, 'rpcRequest:%j', [rpcRequest]);

                apiInterfaces.rpcWallet("sendfrom", rpcRequest, function (error, result) {
                    log('info', logSystem, 'sendto:%j',[transferCmd.rpc.to]);
                    log('info', logSystem, 'sendfrom error:%j', [error]);
                    if (error) {
                        log('info', logSystem, 'Error with %s RPC request to wallet daemon %j', [rpcCommand, error]);
                        log('info', logSystem, 'Payments failed to send to %s', [transferCmd.rpc.to]);
                        cback(false);
                        return;
                    }

                    log('info', logSystem, 'Send to %s success.Hash %s', [transferCmd.rpc.to, result.toString()]);

                    let now = (timeOffset++) + Date.now() / 1000 | 0;
                    let txHash = result.toString('Hex');
                    txHash = txHash.replace('<', '').replace('>', '');

                    transferCmd.redis.push(['zadd', config.coin + ':payments:all', now, [
                        txHash,
                        transferCmd.amount,
                        transferCmd.rpc.txfee,
                        transferCmd.rpc.from,
                        transferCmd.rpc.to/*,
                        transferCmd.rpc.data*/
                    ].join(':')]);

                    log('info', logSystem, 'transferCmd.redisA: %j', [transferCmd.redis]);

                    let notify_miners_on_success = [];
                    for (let i = 0; i < transferCmd.destinations.length; i++) {
                        log('info', logSystem, 'Payments sent via wallet daemon %j', [result]);
                        let destination = transferCmd.destinations[i];
                        if (transferCmd.payment_id) {
                            destination.address += config.poolServer.paymentId.addressSeparator + transferCmd.payment_id;
                        }
                        log('info', logSystem, 'Payments sent via wallet daemon %j', [result]);
                        transferCmd.redis.push(['zadd', config.coin + ':payments:' + destination.address, now, [
                            txHash,
                            destination.amount,
                            config.payments.transferFee,
                            transferCmd.rpc.from,
                            transferCmd.rpc.to/*,
                            transferCmd.rpc.data*/
                        ].join(':')]);
                        log('info', logSystem, 'transferCmd.redis%d: %j', [i,transferCmd.redis]);
                        notify_miners_on_success.push(destination);
                    }
                    
                    redisClient.multi(transferCmd.redis).exec(function (error, replies) {
                        log('info', logSystem, 'redisClient.multi---------------------------');
                        if (error) {
                            log('info', logSystem, 'Super critical error! Payments sent yet failing to update balance in redis, double payouts likely to happen %j', [error]);
                            log('info', logSystem, 'Double payments likely to be sent to %j', transferCmd.destinations);
                            cback(false);
                            return;
                        }

                        log('info', logSystem, 'Send OK: %j', [replies]);

                        for (let m in notify_miners_on_success) {
                            notify_miners.push(notify_miners_on_success[m]);
                        }

                        cback(true);
                    });
                });
            }, function (succeeded) {
                let failedAmount = transferCommands.length - succeeded.length;

                for (let m in notify_miners) {
                    let notify = notify_miners[m];
                    log('info', logSystem, 'Payment of %s to %s', [utils.getReadableCoins(notify.amount), notify.address]);
                    notifications.sendToMiner(notify.address, 'payment', {
                        'ADDRESS': notify.address.substring(0, 7) + '...' + notify.address.substring(notify.address.length - 7),
                        'AMOUNT': utils.getReadableCoins(notify.amount),
                    });
                }
                log('info', logSystem, 'Payments splintered and %d successfully sent, %d failed', [succeeded.length, failedAmount]);

                callback(null);
            });

        }

    ], function (error, result) {
        setTimeout(runInterval, config.payments.interval * 1000);
    });
}

runInterval();
