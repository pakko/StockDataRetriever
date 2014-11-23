var request = require('request');
var fs = require('fs');
var mongo = require('mongodb');
var moment = require('moment');
var async = require('async');

var STOCK_CODES_PATH = './data/corp_codes.txt';
var DB_NAME = 'stock';
var STOCK_TABLE = 'basic';
var STOCK_ADVANCE_TABLE = 'advance';

var stockCodes;
var db;

exports.init = function () {
	//load stock codes first
	stockCodes = fs.readFileSync(STOCK_CODES_PATH).toString().split("\n");
	//stockCodes = ['sz002306','sh600456'];
	console.log('Load Stock Codes ready');
	
	//connect to db
	var Server = mongo.Server;
    var Db = mongo.Db;
	var server = new Server('localhost', 27017, {auto_reconnect: true});
	db = new Db(DB_NAME, server);
	db.open(function(err, db) {
	    if(!err) {
	    	db.collection(STOCK_TABLE).ensureIndex({ code: 1, date: -1 },  { unique: true });
	    	db.collection(STOCK_ADVANCE_TABLE).ensureIndex({ code: 1, date: -1 },  { unique: true });
	        console.log("Connected to 'stock' database");
	    }
	});
	
}

exports.setup = function (app, io) {
	exports.init();
	
	app.get('/api/executor/:type', function (req, res) {
		var type = req.params.type;
		res.json({name: 1000});
	});
	
	var KDailyUrl = "http://api.finance.ifeng.com/index.php/akdaily/?code=";
	var KDailyType = "&type=fq";
	
	var KDailyAdvanceUrl = "http://data1.bestgo.com/stockdata/";
	var KDailyAdvancePost = "/kday.js?";
	
	var retrieveBasic = function(codes, cb) {
		async.each(codes, function(code, callback) {
			var url = '';
			code = code.trim();
			if(code == 'sh000001'){
				url = KDailyUrl + code;
			}
			else{
				url = KDailyUrl + code + KDailyType;
			}
			doRequestBasic(url, code, callback);
		}, function(err) {
			if (err) {
				console.log('Error: ' + err);
			} else {
				io.emit('executor', "Retrieve Basic Task Finished!");
				console.log('Retrieve Basic Task Finished!');
				cb();
			}
		});
	};
	
	var retrieveAdvance = function(codes, cb) {
		async.each(codes, function(code, callback) {
			var date = moment().format('YYYY/MM/DD');
			var url = KDailyAdvanceUrl + code.trim() + KDailyAdvancePost + date;
			
			doRequestAdvance(url, code, callback);
		}, function(err) {
			if (err) {
				console.log('Error: ' + err);
			} else {
				io.emit('executor', "Retrieve Advance Task Finished!");
				console.log('Retrieve Advance Task Finished!');
				cb();
			}
		});
	};
	
	var doRequestBasic = function(url, code, callback) {
		// request data and insert to db
		request(url, function (error, response, body) {
			if (!error && response.statusCode == 200) {
				
				var records = JSON.parse(body).record;
				doProcessBasicData(records, code);
				
				//console.log("Finish basic code " + code);
				callback();
			}
			if(error) {
				console.log('Error occurred when request for code: ' + code);
			}
		});
	};
	
	var doRequestAdvance = function(url, code, callback) {
		// request data and insert to db
		request(url, function (error, response, body) {
			if (!error && response.statusCode == 200) {
				
				eval(body);
				var records = klinedata.split('\n');
				doProcessAdvanceData(records, code);
				
				//console.log("Finish advance code " + code);
				callback();
			}
			if(error) {
				console.log('Error occurred when request for advance code: ' + code);
			}
		});
	};
	
	var doProcessBasicData = function(records, code) {
		for(var i in records) {
			var stockObj = {};
			stockObj.code = code;
			stockObj.date = moment(records[i][0], 'YYYY-MM-DD').unix() * 1000;
			stockObj.opening = parseFloat(records[i][1]);
			stockObj.max = parseFloat(records[i][2]);
			stockObj.close = parseFloat(records[i][3]);
			stockObj.min = parseFloat(records[i][4]);
			stockObj.tradeVolume = parseFloat(records[i][5]);
			stockObj.change = parseFloat(records[i][6]);
			stockObj.changeRate = parseFloat(records[i][7]);
			stockObj.ma5 = parseFloat(records[i][8]);
			stockObj.ma10 = parseFloat(records[i][9]);
			stockObj.ma20 = parseFloat(records[i][10]);
			stockObj.turnOverRate = 0;
			if(records[i].length > 14){
				stockObj.turnOverRate = parseFloat(records[i][14]);
			}
			//console.log(stockObj);
			
			db.collection(STOCK_TABLE).insert(stockObj, {w:1}, function(err, result) {
				if (err) {
	                console.log('Error occurred when insert stock' + err);
	            }
			});
		}
	};
	
	var doProcessAdvanceData = function(records, code) {
		for(var i in records) {
			record = records[i].split(',')
			var stockObj = {};
			stockObj.code = code;
			stockObj.date = moment(record[0], 'YYYY/MM/DD').unix() * 1000;
			stockObj.fund = parseFloat(record[6]);
			stockObj.hide = parseFloat(record[7]);
			stockObj.ddx = parseFloat(record[9]);
			
			db.collection(STOCK_ADVANCE_TABLE).insert(stockObj, {w:1}, function(err, result) {
				if (err) {
	                console.log('Error occurred when insert advance stock' + err);
	            }
			});
		}
	};
	
	io.on('connection', function(socket) {
		socket.on('executor', function(msg) {
			if(msg == 'all'){
				async.series([ function(callback) {
					io.emit('executor', "Executing basic task...");
					retrieveBasic(stockCodes, callback);
				}, function(callback) {
					io.emit('executor', "Executing advance task...");
					retrieveAdvance(stockCodes, callback);
				} ],
				function(err) {
					if (!err) {
						console.log('All task have been processed!');
						io.emit('executor', 'All task have been processed!');
					}
				});
			}
			else if(msg == 'basic'){
				io.emit('executor', "Executing basic task...");
				retrieveBasic(stockCodes, function(){});
			}
			else if(msg == 'advance'){
				io.emit('executor', "Executing advance task...");
				retrieveAdvance(stockCodes, function(){});
			}
			
		});
	});
};
