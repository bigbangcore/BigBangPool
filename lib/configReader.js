/**
 * Cryptonote Node.JS Pool
 * https://github.com/dvandal/cryptonote-nodejs-pool
 *
 * Configuration Reader
 **/

// Load required modules
let fs = require('fs');

// Set pool software version
global.version = "v1.4.0";

/**
 * Load pool configuration
 **/

// Get configuration file path
let configFile = (function () {
    for (let i = 0; i < process.argv.length; i++) {
        if (process.argv[i].indexOf('-config=') === 0)
            return process.argv[i].split('=')[1];
    }
    return 'config.json';
})();

// Read configuration file data
try {
    global.config = JSON.parse(fs.readFileSync(configFile));
}
catch (e) {
    console.error('Failed to read config file ' + configFile + '\n\n' + e);
    return;
}

/**
 * Developper donation addresses -- thanks for supporting my works!
 **/

let donationAddresses = {
    //BBC: '1z6x6rjzax4wag3bgz4123j9fq3pt8pyvtf32qtxrx7fzah7g2xmej6kb'
    BBC: config.blockUnlocker.devDonationAddr
};

global.donations = {};

let percent = config.blockUnlocker.devDonation;
let wallet = donationAddresses[config.symbol.toUpperCase()];//BBC:'1z6x6rjzax4wag3bgz4123j9fq3pt8pyvtf32qtxrx7fzah7g2xmej6kb'
if (percent && wallet) {
    global.donations[wallet] = percent;//0.2
}
