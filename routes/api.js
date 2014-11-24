var request = require('request');
var fs = require('fs');
var mongo = require('mongodb');
var moment = require('moment');
var async = require('async');

var STOCK_CODES_PATH = './data/corp_codes.txt';
var DB_NAME = 'sdr';
var STOCK_TABLE = 'basic';
var STOCK_ADVANCE_TABLE = 'advance';
var TRANSFER_BASIC_TABLE = 'transfer_basic';
var TRANSFER_ADVANCE_TABLE = 'transfer_advance';

var stockCodes;
var db;
var dates;

exports.init = function () {
	//load stock codes first
	stockCodes = fs.readFileSync(STOCK_CODES_PATH).toString().split("\n");
	//stockCodes = ['sh000001', 'sh600283','sz002363'];
	console.log('Load Stock Codes ready');
	
	//load transfer dates
	var begin = '2014-01-01';
	var end = moment().format('YYYY-MM-DD');
	dates = getWorkingDays(begin, end);
	console.log('Load Transfer dates ready, from ' + begin + ' to ' + end);
	
	//connect to db
	var Server = mongo.Server;
    var Db = mongo.Db;
	var server = new Server('10.74.68.13', 27017, {auto_reconnect: true});
	db = new Db(DB_NAME, server);
	db.open(function(err, db) {
	    if(!err) {
	    	db.collection(STOCK_TABLE).ensureIndex({ code: 1, date: -1 },  { unique: true });
	    	db.collection(STOCK_ADVANCE_TABLE).ensureIndex({ code: 1, date: -1 },  { unique: true });
			db.collection(TRANSFER_BASIC_TABLE).ensureIndex({ code: 1, date: -1 },  { unique: true });
			db.collection(TRANSFER_ADVANCE_TABLE).ensureIndex({ code: 1, date: -1 },  { unique: true });
	        console.log("Connected to 'stock' database");
	    }
	});
	
};

function getWorkingDays(startDate, endDate) {
	var startM = moment(startDate, 'YYYY-MM-DD');
	var endM = moment(endDate, 'YYYY-MM-DD');
	
	var days = getDaysBetween(startM.unix() * 1000, endM.unix() * 1000);
	
	var dateList = [];
	for(var i = 0; i < days; i++) {
		var date = startM.format('YYYY-MM-DD');
		dateList.push(date);
		startM.add(1, 'days');
	}
	return dateList;
};
	
function getDaysBetween(startDate, endDate) {
	return (endDate - startDate) / (1000*3600*24) + 1;
};
	
exports.setup = function (app, io) {
	exports.init();
	
	app.get('/api/viewer/:code', function (req, res) {
		var code = req.params.code;
		//res.json({name: 1000});
		db.collection(TRANSFER_BASIC_TABLE).find({"code": code}).sort({"date": -1}).toArray(function(err, result) {
			if (!err) {
				res.json(result);
			}
		});
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
	
	var transferBasic = function(codes, dates, cb) {
		async.each(dates, function(date, callback1) {
			async.each(codes, function(code, callback2) {
				var d = moment(date, 'YYYY-MM-DD').unix() * 1000;
				doTransferBasic(code, d, callback2);
			}, function(err) {
				if (err) {
					console.log('Error: ' + err);
				}
				callback1();
			});
		}, function(err) {
			if (err) {
				console.log('Error: ' + err);
			} else {
				io.emit('executor', "Transfer Basic Task Finished!");
				console.log('Transfer Basic Task Finished!');
				cb();
			}
		});
	};
	
	var transferAdvance = function(codes, dates, cb) {
		async.each(dates, function(date, callback1) {
			async.each(codes, function(code, callback2) {
				var d = moment(date, 'YYYY-MM-DD').unix() * 1000;
				doTransferAdvance(code, d, callback2);
			}, function(err) {
				if (err) {
					console.log('Error: ' + err);
				}
				callback1();
			});
		}, function(err) {
			if (err) {
				console.log('Error: ' + err);
			} else {
				io.emit('executor', "Transfer Advance Task Finished!");
				console.log('Transfer Advance Task Finished!');
				cb();
			}
		});
	};
	
	var doTransferBasic = function(code, date, callback) {
		db.collection(STOCK_TABLE).find({"code": code, "date": {$lte: date}}).sort({"date": -1}).toArray(function(err, result) {
			if (!err) {
				var stockList = result;
				
				var obj = {};
				obj.code = code;
				obj.date = date;
				
				//1, calculate average price
				obj.ma = getDaysOfAveragePrice(stockList, 1);
				obj.ma5 = getDaysOfAveragePrice(stockList, 5);
				obj.ma10 = getDaysOfAveragePrice(stockList, 10);
				obj.ma20 = getDaysOfAveragePrice(stockList, 20);
				obj.ma30 = getDaysOfAveragePrice(stockList, 30);
				obj.ma60 = getDaysOfAveragePrice(stockList, 60);
				obj.ma60 = getDaysOfAveragePrice(stockList, 90);
				obj.ma120 = getDaysOfAveragePrice(stockList, 120);
				
				//2, calculate hsl
				obj.hsl = getDaysOfTurnOverRate(stockList, 1);
				obj.hsl5 = getDaysOfTurnOverRate(stockList, 5);
				obj.hsl10 = getDaysOfTurnOverRate(stockList, 10);
				obj.hsl20 = getDaysOfTurnOverRate(stockList, 20);
				obj.hsl30 = getDaysOfTurnOverRate(stockList, 30);
				obj.hsl60 = getDaysOfTurnOverRate(stockList, 60);
				obj.hsl90 = getDaysOfTurnOverRate(stockList, 90);
				obj.hsl120 = getDaysOfTurnOverRate(stockList, 120);

				//3, calculate up
				obj.up = getDaysOfUp(stockList, 1);
				obj.up5 = getDaysOfUp(stockList, 5);
				obj.up10 = getDaysOfUp(stockList, 10);
				obj.up20 = getDaysOfUp(stockList, 20);
				obj.up30 = getDaysOfUp(stockList, 30);
				obj.up60 = getDaysOfUp(stockList, 60);
				obj.up90 = getDaysOfUp(stockList, 90);
				obj.up120 = getDaysOfUp(stockList, 120);
				
				db.collection(TRANSFER_BASIC_TABLE).insert(obj, {w:1}, function(err, result) {
					if (err) {
						console.log('Error occurred when insert stock' + err);
					}
				});
			}
			callback();
		});
	};
	
	var doTransferAdvance = function(code, date, callback) {
		db.collection(STOCK_ADVANCE_TABLE).find({"code": code, "date": {$lte: date}}).sort({"date": -1}).toArray(function(err, result) {
			if (!err) {
				var stockList = result;
				
				var obj = {};
				obj.code = code;
				obj.date = date;
				
				//1, calculate ddx
				obj.ddx = getDaysOfDDX(stockList, 1);
				obj.ddx5 = getDaysOfDDX(stockList, 5);
				obj.ddx10 = getDaysOfDDX(stockList, 10);
				obj.ddx20 = getDaysOfDDX(stockList, 20);
				obj.ddx30 = getDaysOfDDX(stockList, 30);
				obj.ddx60 = getDaysOfDDX(stockList, 60);
				obj.ddx60 = getDaysOfDDX(stockList, 90);
				obj.ddx120 = getDaysOfDDX(stockList, 120);
				
				//2, calculate positive ddx
				obj.pddx = getDaysOfPositiveDDX(stockList, 1);
				obj.pddx5 = getDaysOfPositiveDDX(stockList, 5);
				obj.pddx10 = getDaysOfPositiveDDX(stockList, 10);
				obj.pddx20 = getDaysOfPositiveDDX(stockList, 20);
				obj.pddx30 = getDaysOfPositiveDDX(stockList, 30);
				obj.pddx60 = getDaysOfPositiveDDX(stockList, 60);
				obj.pddx60 = getDaysOfPositiveDDX(stockList, 90);
				obj.pddx120 = getDaysOfPositiveDDX(stockList, 120);
				
				//3, fund intensity
				obj.fund = getDaysOfFundIntensity(stockList, 1);
				obj.fund5 = getDaysOfFundIntensity(stockList, 5);
				obj.fund10 = getDaysOfFundIntensity(stockList, 10);
				obj.fund20 = getDaysOfFundIntensity(stockList, 20);
				obj.fund30 = getDaysOfFundIntensity(stockList, 30);
				obj.fund60 = getDaysOfFundIntensity(stockList, 60);
				obj.fund90 = getDaysOfFundIntensity(stockList, 90);
				obj.fund120 = getDaysOfFundIntensity(stockList, 120);
				
				//4, positive fund intensity
				obj.pfund = getDaysOfPositiveFundIntensity(stockList, 1);
				obj.pfund5 = getDaysOfPositiveFundIntensity(stockList, 5);
				obj.pfund10 = getDaysOfPositiveFundIntensity(stockList, 10);
				obj.pfund20 = getDaysOfPositiveFundIntensity(stockList, 20);
				obj.pfund30 = getDaysOfPositiveFundIntensity(stockList, 30);
				obj.pfund60 = getDaysOfPositiveFundIntensity(stockList, 60);
				obj.pfund90 = getDaysOfPositiveFundIntensity(stockList, 90);
				obj.pfund120 = getDaysOfPositiveFundIntensity(stockList, 120);

				//5, calculate hide intensity
				obj.hide = getDaysOfHideIntensity(stockList, 1);
				obj.hide5 = getDaysOfHideIntensity(stockList, 5);
				obj.hide10 = getDaysOfHideIntensity(stockList, 10);
				obj.hide20 = getDaysOfHideIntensity(stockList, 20);
				obj.hide30 = getDaysOfHideIntensity(stockList, 30);
				obj.hide60 = getDaysOfHideIntensity(stockList, 60);
				obj.hide90 = getDaysOfHideIntensity(stockList, 90);
				obj.hide120 = getDaysOfHideIntensity(stockList, 120);
				
				//6, positive hide intensity
				obj.phide = getDaysOfPositiveHideIntensity(stockList, 1);
				obj.phide5 = getDaysOfPositiveHideIntensity(stockList, 5);
				obj.phide10 = getDaysOfPositiveHideIntensity(stockList, 10);
				obj.phide20 = getDaysOfPositiveHideIntensity(stockList, 20);
				obj.phide30 = getDaysOfPositiveHideIntensity(stockList, 30);
				obj.phide60 = getDaysOfPositiveHideIntensity(stockList, 60);
				obj.phide90 = getDaysOfPositiveHideIntensity(stockList, 90);
				obj.phide120 = getDaysOfPositiveHideIntensity(stockList, 120);
				
				db.collection(TRANSFER_ADVANCE_TABLE).insert(obj, {w:1}, function(err, result) {
					if (err) {
						console.log('Error occurred when insert stock' + err);
					}
				});
			}
			callback();
		});
	};
	
	var doRequestBasic = function(url, code, callback) {
		// request data and insert to db
		request(url, function (error, response, body) {
			if (!error && response.statusCode == 200) {
				
				var records = JSON.parse(body).record;
				doProcessBasicData(records, code);
				
				//console.log("Finish basic code " + code);
			}
			if(error) {
				console.log('Error occurred when request for code: ' + code);
			}
			callback();
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
			}
			if(error) {
				console.log('Error occurred when request for advance code: ' + code);
			}
			callback();
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
				}, function(callback) {
					io.emit('executor', "Executing transfer basic task...");
					transferBasic(stockCodes, dates, callback);
				}, function(callback) {
					io.emit('executor', "Executing transfer advance task...");
					transferAdvance(stockCodes, dates, callback);
				} 
				],
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
			else if(msg == 'transfer_basic'){
				io.emit('executor', "Executing transfer basic task...");
				transferBasic(stockCodes, dates, function(){});
			}
			else if(msg == 'transfer_advance'){
				io.emit('executor', "Executing transfer advance task...");
				transferAdvance(stockCodes, dates, function(){});
			}
			
			
		});
	});
	
	var getDaysOfDDX = function (stockList, days) {
		var ap = 0.0;
		if (stockList.length >= days) {
			for (var i = 0; i < days; i++) {
				ap += stockList[i].ddx;
			}
			ap = ap / days;
		}
		return ap;
	}
	
	var getDaysOfPositiveDDX = function (stockList, days) {
		var ap = 0;
		if (stockList.length >= days) {
			for (var i = 0; i < days; i++) {
				if(stockList[i].ddx > 0) {
					ap++;
				}
			}
		}
		return ap;
	}
	
	var getDaysOfFundIntensity = function (stockList, days) {
		var ap = 0.0;
		if (stockList.length >= days) {
			for (var i = 0; i < days; i++) {
				ap += stockList[i].fund;
			}
			ap = ap / days;
		}
		return ap;
	}
	
	var getDaysOfPositiveFundIntensity = function (stockList, days) {
		var ap = 0;
		if (stockList.length >= days) {
			for (var i = 0; i < days; i++) {
				if(stockList[i].fund > 0) {
					ap++;
				}
			}
		}
		return ap;
	}
	
	var getDaysOfHideIntensity = function (stockList, days) {
		var ap = 0.0;
		if (stockList.length >= days) {
			for (var i = 0; i < days; i++) {
				ap += stockList[i].hide;
			}
			ap = ap / days;
		}
		return ap;
	}
	
	var getDaysOfPositiveHideIntensity = function (stockList, days) {
		var ap = 0;
		if (stockList.length >= days) {
			for (var i = 0; i < days; i++) {
				if(stockList[i].hide > 0) {
					ap++;
				}
			}
		}
		return ap;
	}
	
	var getDaysOfAveragePrice = function (stockList, days) {
		var ap = 0.0;
		if (stockList.length >= days) {
			for (var i = 0; i < days; i++) {
				ap += stockList[i].close;
			}
			ap = ap / days;
		}
		return ap;
	}
	
	var getDaysOfTurnOverRate = function (stockList, days) {
		var avgTurnOverRate = 0.0;
		if (stockList.length >= days) {
			for (var i = 0; i < days; i++) {
				avgTurnOverRate += stockList[i].turnOverRate;
			}
			avgTurnOverRate = avgTurnOverRate / days;
		}
		return avgTurnOverRate;
	}
	
	var getDaysOfUp = function (stockList, days) {
		var up = 0.0;
		if (stockList.length >= days) {
			for (var i = 0; i < days; i++) {
				up += stockList[i].changeRate;
			}
			up = up / days;
		}
		return up;
	}
};
