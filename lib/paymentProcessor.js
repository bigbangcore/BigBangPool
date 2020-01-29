/**
 * Cryptonote Node.JS Pool
 * https://github.com/dvandal/cryptonote-nodejs-pool
 *
 * Payments processor
 **/

// Load required modules
let fs = require('fs');
let async = require('async');
let schedule = require('node-schedule');

let apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet, config.api);
let notifications = require('./notifications.js');
let utils = require('./utils.js');

// Initialize log system
let logSystem = 'payments';
require('./exceptionWriter.js')(logSystem);

/**
 * Run payments processor
 **/

log('info', logSystem, 'Pay start ... ');


if (!config.poolServer.paymentId) config.poolServer.paymentId = {};
if (!config.poolServer.paymentId.addressSeparator) config.poolServer.paymentId.addressSeparator = "+";
if (!config.payments.priority) config.payments.priority = 0;

let scheduleTime = config.payments.ScheduleTime || 0;
let rule = new schedule.RecurrenceRule();

function runInterval() {
    async.waterfall([
        // Get worker keys
        function (callback) {
            redisClient.keys(config.coin + ':workers:*', function (error, result) {
                if (error) {
                    log('error', logSystem, 'Error trying to get worker balances:%j', [error]);
                    callback(true);
                    return;
                }
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
                    log('error', logSystem, 'Error with getting balances:%j', [error]);
                    callback(true);
                    return;
                }

                let balances = {};
                for (let i = 0; i < replies.length; i++) {
                    let parts = keys[i].split(':');
                    let workerId = parts[parts.length - 1];
                    balances[workerId] = utils.toFixed(replies[i]) || 0;                   
                }
                //log('info', logSystem, 'balances:%j', [balances]);
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
                    log('error', logSystem, 'Error with getting minimum payout:%j', [error]);   
                    callback(true);
                    return;                 
                }                

                let minPayoutLevel = {};
                for (let i = 0; i < replies.length; i++) {
                    let parts = keys[i].split(':');
                    let workerId = parts[parts.length - 1];

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
                //log('info', logSystem, 'balanceB:%j',[balance]);
                //log('info', logSystem, 'workerB:%j',[worker]);
                //log('info', logSystem, 'minPayoutLevel[worker]:%j',[minPayoutLevel[worker]]);
                if (balance >= minPayoutLevel[worker]) {
                    let remainder = balance % config.payments.denomination;
                    let payout = balance - remainder;

                    if (config.payments.dynamicTransferFee && config.payments.minerPayFee) {
                        payout -= config.payments.transferFee;
                    }
                    if (payout < 0) continue;
                    payments[worker] = utils.toFixed(payout,6);
                    //log('info', logSystem, 'payout:%j',[payout]);
                }
            }

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
                let amount = payments[worker];
                if (config.payments.maxTransactionAmount && (amount + commandAmount) > config.payments.maxTransactionAmount) {
                    amount = config.payments.maxTransactionAmount - commandAmount;
                }                

                let address = worker;
                let payment_id = null;
                let with_payment_id = false;
                let addr = address.split(config.poolServer.paymentId.addressSeparator);

                //if ((addr.length === 1 && utils.isIntegratedAddress(address)) || addr.length >= 2){
                with_payment_id = true;
                if (addr.length >= 1) {
                    address = addr[0];
                    payment_id = addr[0];
                    //payment_id = payment_id.replace(/[^A-Za-z0-9]/g, '');
                    if (payment_id.length !== 16 && (address.length < 50 || address.length > 64)) {
                        with_payment_id = false;
                        payment_id = null;
                    }
                }

                if (addresses > 0) {
                    commandIndex++;
                    addresses = 0;
                    commandAmount = 0;
                }

                if (config.poolServer.fixedDiff && config.poolServer.fixedDiff.enabled) {
                    let addr = address.split(config.poolServer.fixedDiff.addressSeparator);
                    if (addr.length >= 1) address = addr[0];
                }

                if (!transferCommands[commandIndex]) {
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
                }

                //给transferCommands的各个参数设置值
                amount = Number(utils.toFixed(amount,6));
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

                if (config.payments.dynamicTransferFee) {
                    transferCommands[commandIndex].rpc.txfee = config.payments.transferFee * addresses;
                }

                if (addresses >= config.payments.maxAddresses || (config.payments.maxTransactionAmount && commandAmount >= config.payments.maxTransactionAmount) || with_payment_id) {
                    commandIndex++;
                    addresses = 0;
                    commandAmount = 0;
                }
            }

            let timeOffset = 0;
            let notify_miners = [];

            // unlockkey
            let unlock_rpcRequest = {
                pubkey:config.poolServer.spentPubkey,
                passphrase:config.poolServer.spentPWD,
                timeout:config.poolServer.unlockTimeout //30s
            }

            apiInterfaces.rpcWallet('unlockkey', unlock_rpcRequest, function(error, pubKey){
                if (parseInt(error.code) === -406){
                    log("error",logSystem,"unlockkey fail:%j",[error]);
                    callback(true);
                    return;                   
                }
                //let daemonType = config.daemonType ? config.daemonType.toLowerCase() : "default";

                async.filter(transferCommands, function (transferCmd, cback) {
                    let rpcRequest = transferCmd.rpc;
    
                    //log('error', logSystem, 'rpcRequest:%j', [rpcRequest]);
    
                    apiInterfaces.rpcWallet("sendfrom", rpcRequest, function (error, result) {
                        if (error) {
                            log('info', logSystem, 'Error RPC : %j , error : %j', [rpcRequest, error]);
                            cback(false);
                            return;
                        }
    
                        //log('info', logSystem, 'Send to %s, success.Hash:%s, amount:%j', [transferCmd.rpc.to, result.toString(), transferCmd.rpc.amount]);
    
                        let now = (timeOffset++) + Date.now() / 1000 | 0;
                        let txHash = result.toString('Hex');
                        txHash = txHash.replace('<', '').replace('>', '');
    
                        //TODO:USE MySQL to DO IT
                        transferCmd.redis.push(['zadd', config.coin + ':payments:all', now, [
                            txHash,
                            transferCmd.amount,
                            transferCmd.rpc.txfee,
                            transferCmd.rpc.from,
                            transferCmd.rpc.to/*,
                            transferCmd.rpc.data*/
                        ].join(':')]);
    
                        let notify_miners_on_success = [];
                        for (let i = 0; i < transferCmd.destinations.length; i++) {
                            let destination = transferCmd.destinations[i];
                            if (transferCmd.payment_id) {
                                destination.address += config.poolServer.paymentId.addressSeparator + transferCmd.payment_id;
                            }
                            //log('info', logSystem, 'Payments sent via wallet daemon %j', [result]);
                            //TODO:USE MySQL to DO IT
                            transferCmd.redis.push(['zadd', config.coin + ':payments:' + destination.address, now, [
                                txHash,
                                destination.amount,
                                config.payments.transferFee,
                                transferCmd.rpc.from,
                                transferCmd.rpc.to/*,
                                transferCmd.rpc.data*/
                            ].join(':')]);
                            notify_miners_on_success.push(destination);
                        }
                        
                        redisClient.multi(transferCmd.redis).exec(function (error, replies) {
                            if (error) {
                                log('info', logSystem, 'Super critical error! Payments sent yet failing to update balance in database, double payouts likely to happen %j', [error]);
                                log('info', logSystem, 'Double payments likely to be sent to %j', [transferCmd.destinations]);
                                cback(false);
                                return;
                            }
    
                            //log('info', logSystem, 'Send OK: %j', [replies]);
    
                            for (let m in notify_miners_on_success) {
                                notify_miners.push(notify_miners_on_success[m]);
                            }
                            cback(true);
                        });                        
                    });                    
                }, function (succeeded) {
                    let failedAmount = transferCommands.length - succeeded.length;
    
                    let totalAmount = 0;
                    for (let m in notify_miners) {
                        let notify = notify_miners[m];
                        let tmpAmount = parseFloat(utils.toFixed(notify.amount,6));
                        totalAmount += tmpAmount;
                        let addr = notify.address;
                        if (notify.address.indexOf('+') > -1){
                            addr = notify.address.split('+')[0];
                        }
                        log('info', logSystem, 'Payment of %s to %s', [tmpAmount, addr]);
                        notifications.sendToMiner(addr, 'payment', {
                            'ADDRESS': addr.substring(0, 7) + '...' + addr.substring(addr.length - 7),
                            'AMOUNT': utils.getReadableCoins(notify.amount),
                        });
                    }

                    log('info', logSystem, 'Payments splintered and %d successfully sent, %d failed', [succeeded.length, failedAmount]);

                    apiInterfaces.rpcWallet('lockkey', {pubkey:config.poolServer.spentPubkey}, function(error, result){
                        if (result.toString().indexOf('successfully') === -1){
                            log("error",logSystem,"lockkey fail:%j", [result]);
                        } else {
                            log("info",logSystem,"lockkey OK:%j", [result]);
                        }
                    });
                });
            });

            callback(null,"OK");
        }
    ], function (error, result) {
        if (error){
            log("info",logSystem,"pay error");
        } 

        if (!scheduleTime){
            setTimeout(runInterval, config.payments.interval * 1000);//30m once
        }        
    });
}

//npm install node-schedule
if (scheduleTime) {    
    let arr = scheduleTime.split(':');
    rule.hour = parseInt(arr[0]);
    rule.minute = parseInt(arr[1]) > 59 ? 59 : parseInt(arr[1]);
    rule.second = parseInt(arr[2]) > 59 ? 59 : parseInt(arr[2]);

    schedule.scheduleJob(rule, function () {        
        log('info', logSystem, 'Start to pay ... ');
        runInterval();
    });
}else{
    runInterval();
}