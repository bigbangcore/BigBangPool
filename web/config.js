var api = "";
//var api = "http://18.139.37.48:8117";
var explorHost = "http://www.bbcexplorer.com";

//获取页面完整地址
var url = window.location.href;
if(url.lastIndexOf('/') > 8){
    api = url.substring(0,url.lastIndexOf('/')) + ":8117";
}
else{
    api = '';
}

var email = "";
var telegram = "";
var facebook = "";

var marketCurrencies = ["{symbol}-BTC", "{symbol}-LTC", "{symbol}-DOGE", "{symbol}-USDT", "{symbol}-USD", "{symbol}-EUR", "{symbol}-CAD"];

var blockchainExplorer = "###";
var transactionExplorer = "###";;

var themeCss = "themes/default.css";
var defaultLang = 'en';
