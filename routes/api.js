var request = require('request');
var fs = require('fs');
var mongo = require('mongodb');
var async = require('async');
var SimpleJson2Csv = require('simple-json2csv');

var STOCK_CODES_PATH = './data/corp_codes.txt';
var COMBINE_DATA_PATH = './data/generated_data';
var DB_NAME = 'stock';
var STOCK_BASIC_TABLE = 'basic';
var STOCK_ADVANCE_TABLE = 'advance';
var TRANSFER_BASIC_TABLE = 'transfer_basic';
var TRANSFER_ADVANCE_TABLE = 'transfer_advance';

var TRANSFER_START_DATE = '2014-10-01';
var MONGO_HOST = 'localhost';

var stockCodes;
var db;
var dates;

var CSV_FIELDS = [
	{"name": "code", "header": "code"},
	{"name": "date", "header": "date"},
	
	{"name": "ma", "header": "ma"},
	{"name": "ma5", "header": "ma5"},
	{"name": "ma10", "header": "ma10"},
	{"name": "ma20", "header": "ma20"},
	{"name": "ma30", "header": "ma30"},
	{"name": "ma60", "header": "ma60"},
	{"name": "ma90", "header": "ma90"},
	{"name": "ma120", "header": "ma120"},
	
	{"name": "hsl", "header": "hsl"},
	{"name": "hsl5", "header": "hsl5"},
	{"name": "hsl10", "header": "hsl10"},
	{"name": "hsl20", "header": "hsl20"},
	{"name": "hsl30", "header": "hsl30"},
	{"name": "hsl60", "header": "hsl60"},
	{"name": "hsl90", "header": "hsl90"},
	{"name": "hsl120", "header": "hsl120"},
	
	{"name": "up", "header": "up"},
	{"name": "up5", "header": "up5"},
	{"name": "up10", "header": "up10"},
	{"name": "up20", "header": "up20"},
	{"name": "up30", "header": "up30"},
	{"name": "up60", "header": "up60"},
	{"name": "up90", "header": "up90"},
	{"name": "up120", "header": "up120"},
	
	{"name": "szindex", "header": "szindex"},
	{"name": "szindex5", "header": "szindex5"},
	{"name": "szindex10", "header": "szindex10"},
	{"name": "szindex20", "header": "szindex20"},
	{"name": "szindex30", "header": "szindex30"},
	{"name": "szindex60", "header": "szindex60"},
	{"name": "szindex90", "header": "szindex90"},
	{"name": "szindex120", "header": "szindex120"},
	
	{"name": "ddx", "header": "ddx"},
	{"name": "ddx5", "header": "ddx5"},
	{"name": "ddx10", "header": "ddx10"},
	{"name": "ddx20", "header": "ddx20"},
	{"name": "ddx30", "header": "ddx30"},
	{"name": "ddx60", "header": "ddx60"},
	{"name": "ddx90", "header": "ddx90"},
	
	{"name": "pddx", "header": "pddx"},
	{"name": "pddx5", "header": "pddx5"},
	{"name": "pddx10", "header": "pddx10"},
	{"name": "pddx20", "header": "pddx20"},
	{"name": "pddx30", "header": "pddx30"},
	{"name": "pddx60", "header": "pddx60"},
	{"name": "pddx90", "header": "pddx90"},
	
	{"name": "fund", "header": "fund"},
	{"name": "fund5", "header": "fund5"},
	{"name": "fund10", "header": "fund10"},
	{"name": "fund20", "header": "fund20"},
	{"name": "fund30", "header": "fund30"},
	{"name": "fund60", "header": "fund60"},
	{"name": "fund90", "header": "fund90"},
	
	{"name": "pfund", "header": "pfund"},
	{"name": "pfund5", "header": "pfund5"},
	{"name": "pfund10", "header": "pfund10"},
	{"name": "pfund20", "header": "pfund20"},
	{"name": "pfund30", "header": "pfund30"},
	{"name": "pfund60", "header": "pfund60"},
	{"name": "pfund90", "header": "pfund90"},
	
	{"name": "hide", "header": "hide"},
	{"name": "hide5", "header": "hide5"},
	{"name": "hide10", "header": "hide10"},
	{"name": "hide20", "header": "hide20"},
	{"name": "hide30", "header": "hide30"},
	{"name": "hide60", "header": "hide60"},
	{"name": "hide90", "header": "hide90"},
	
	{"name": "phide", "header": "phide"},
	{"name": "phide5", "header": "phide5"},
	{"name": "phide10", "header": "phide10"},
	{"name": "phide20", "header": "phide20"},
	{"name": "phide30", "header": "phide30"},
	{"name": "phide60", "header": "phide60"},
	{"name": "phide90", "header": "phide90"},
	
	{"name": "", "header": ""},
	{"name": "discretema", "header": "discretema"},
	{"name": "discretema5", "header": "discretema5"},
	{"name": "discretema10", "header": "discretema10"},
	{"name": "discretema20", "header": "discretema20"},
	{"name": "discretema30", "header": "discretema30"},
	{"name": "discretema60", "header": "discretema60"},
	{"name": "discretema90", "header": "discretema90"},
	
	{"name": "discretehsl", "header": "discretehsl"},
	{"name": "discretehsl5", "header": "discretehsl5"},
	{"name": "discretehsl10", "header": "discretehsl10"},
	{"name": "discretehsl20", "header": "discretehsl20"},
	{"name": "discretehsl30", "header": "discretehsl30"},
	{"name": "discretehsl60", "header": "discretehsl60"},
	{"name": "discretehsl90", "header": "discretehsl90"},
	
	{"name": "discreteup", "header": "discreteup"},
	{"name": "discreteup5", "header": "discreteup5"},
	{"name": "discreteup10", "header": "discreteup10"},
	{"name": "discreteup20", "header": "discreteup20"},
	{"name": "discreteup30", "header": "discreteup30"},
	{"name": "discreteup60", "header": "discreteup60"},
	{"name": "discreteup90", "header": "discreteup90"},
	
	{"name": "discreteszindex", "header": "discreteszindex"},
	{"name": "discreteszindex5", "header": "discreteszindex5"},
	{"name": "discreteszindex10", "header": "discreteszindex10"},
	{"name": "discreteszindex20", "header": "discreteszindex20"},
	{"name": "discreteszindex30", "header": "discreteszindex30"},
	{"name": "discreteszindex60", "header": "discreteszindex60"},
	{"name": "discreteszindex90", "header": "discreteszindex90"},
	
	{"name": "discreteddx", "header": "discreteddx"},
	{"name": "discreteddx5", "header": "discreteddx5"},
	{"name": "discreteddx10", "header": "discreteddx10"},
	{"name": "discreteddx20", "header": "discreteddx20"},
	{"name": "discreteddx30", "header": "discreteddx30"},
	{"name": "discreteddx60", "header": "discreteddx60"},
	{"name": "discreteddx90", "header": "discreteddx90"},
	
	{"name": "discretepddx", "header": "discretepddx"},
	{"name": "discretepddx5", "header": "discretepddx5"},
	{"name": "discretepddx10", "header": "discretepddx10"},
	{"name": "discretepddx20", "header": "discretepddx20"},
	{"name": "discretepddx30", "header": "discretepddx30"},
	{"name": "discretepddx60", "header": "discretepddx60"},
	{"name": "discretepddx90", "header": "discretepddx90"},
	
	{"name": "discretefund", "header": "discretefund"},
	{"name": "discretefund5", "header": "discretefund5"},
	{"name": "discretefund10", "header": "discretefund10"},
	{"name": "discretefund20", "header": "discretefund20"},
	{"name": "discretefund30", "header": "discretefund30"},
	{"name": "discretefund60", "header": "discretefund60"},
	{"name": "discretefund90", "header": "discretefund90"},
	
	{"name": "discretepfund", "header": "discretepfund"},
	{"name": "discretepfund5", "header": "discretepfund5"},
	{"name": "discretepfund10", "header": "discretepfund10"},
	{"name": "discretepfund20", "header": "discretepfund20"},
	{"name": "discretepfund30", "header": "discretepfund30"},
	{"name": "discretepfund60", "header": "discretepfund60"},
	{"name": "discretepfund90", "header": "discretepfund90"},
	
	{"name": "discretehide", "header": "discretehide"},
	{"name": "discretehide5", "header": "discretehide5"},
	{"name": "discretehide10", "header": "discretehide10"},
	{"name": "discretehide20", "header": "discretehide20"},
	{"name": "discretehide30", "header": "discretehide30"},
	{"name": "discretehide60", "header": "discretehide60"},
	{"name": "discretehide90", "header": "discretehide90"},
	
	{"name": "discretephide", "header": "discretephide"},
	{"name": "discretephide5", "header": "discretephide5"},
	{"name": "discretephide10", "header": "discretephide10"},
	{"name": "discretephide20", "header": "discretephide20"},
	{"name": "discretephide30", "header": "discretephide30"},
	{"name": "discretephide60", "header": "discretephide60"},
	{"name": "discretephide90", "header": "discretephide90"},
	
	
	{"name": "", "header": ""},
	{"name": "positive", "header": "positive"}
];

var HOLIDAYS = ["2012-01-02", "2012-01-03", "2012-01-23", "2012-01-24", "2012-01-25", "2012-01-26", "2012-01-27", "2012-04-02", "2012-04-03", "2012-04-04", "2012-04-30", "2012-05-01", "2012-05-02", "2012-06-22", "2012-10-01", "2012-10-02", "2012-10-03", "2012-10-04", "2012-10-05",
                "2013-01-01", "2013-01-02", "2013-01-03", "2013-02-11", "2013-02-12", "2013-02-13", "2013-02-14", "2013-02-15", "2013-04-04", "2013-04-05", "2013-04-29", "2013-04-30", "2013-05-01", "2013-06-10", "2013-06-11", "2013-06-12", "2013-09-19", "2013-09-20", "2013-10-01", "2013-10-02", "2013-10-03", "2013-10-04", "2013-10-07",
                "2014-01-01", "2014-01-31", "2014-02-01", "2014-02-02", "2014-02-03", "2014-02-04", "2014-02-05", "2014-02-06", "2014-04-07", "2014-05-01", "2014-05-02", "2014-05-03", "2014-06-02", "2014-09-08", "2014-10-01", "2014-10-02", "2014-10-03", "2014-10-04", "2014-10-05", "2014-10-06", "2014-10-07"];

exports.init = function () {
	//load stock codes first
	//stockCodes = fs.readFileSync(STOCK_CODES_PATH).toString().split("\n");
	stockCodes = ['sh000001', 'sz000709'];
	console.log('Load Stock Codes ready');
	
	//load transfer dates
	//var begin = '2013-01-01';
	var end = new Date().format('YYYY-MM-DD');
	dates = getWorkingDays(TRANSFER_START_DATE, end);
	console.log('Load Transfer dates ready, from ' + TRANSFER_START_DATE + ' to ' + end);
	
	//connect to db
	var Server = mongo.Server;
    var Db = mongo.Db;
	var server = new Server(MONGO_HOST, 27017, {auto_reconnect: true});
	db = new Db(DB_NAME, server);
	db.open(function(err, db) {
	    if(!err) {
	    	db.collection(STOCK_BASIC_TABLE).ensureIndex({ code: 1, date: -1 },  { unique: true });
	    	db.collection(STOCK_ADVANCE_TABLE).ensureIndex({ code: 1, date: -1 },  { unique: true });
			db.collection(TRANSFER_BASIC_TABLE).ensureIndex({ code: 1, date: -1 },  { unique: true });
			db.collection(TRANSFER_ADVANCE_TABLE).ensureIndex({ code: 1, date: -1 },  { unique: true });
	        console.log("Connected to 'stock' database");
	    }
	});
};

function isHolidayOrWeekend(date) {
	var day = new Date(date).getDay();
	if(day == 0 || day == 6) {
		return true;
	}
	//if date is holiday, return true
	if(HOLIDAYS.toString().indexOf(date) != -1) {
		return true;
	}
	return false;
}

function getWorkingDays(startDate, endDate) {
	var start = new Date(startDate);
	var end = new Date(endDate);
	
	var days = getDaysBetween(start.getTime(), end.getTime());
	
	var dateList = [];
	for(var i = 0; i < days; i++) {
		var date = start.format('YYYY-MM-DD');
		if(!isHolidayOrWeekend(date)){
			dateList.push(date);
		}
		start.setDate(start.getDate() + 1);
	}
	return dateList;
}
	
function getDaysBetween(startDate, endDate) {
	return (endDate - startDate) / (1000*3600*24) + 1;
}

function getAfterWorkingDay(date, days) {
	var d = new Date(date);
	d.setDate(d.getDate() + days);
	while(isHolidayOrWeekend(d.format('YYYY-MM-DD'))){
		d.setDate(d.getDate() + 1);
	}
	return d.getTime();
}

function extendObj(o, n, override) {
	for ( var p in n){
		if (n.hasOwnProperty(p) && (!o.hasOwnProperty(p) || override)) {
			o[p] = n[p];
		}
	}
}

Date.prototype.format=function(fmt) {         
    var o = {         
    "M+" : this.getMonth()+1,      
    "D+" : this.getDate(),
    "h+" : this.getHours()%12 == 0 ? 12 : this.getHours()%12,
    "H+" : this.getHours(),
    "m+" : this.getMinutes(), 
    "s+" : this.getSeconds(),
    "q+" : Math.floor((this.getMonth()+3)/3), 
    "S" : this.getMilliseconds()
    };         
    var week = {         
    "0" : "/u65e5",         
    "1" : "/u4e00",         
    "2" : "/u4e8c",         
    "3" : "/u4e09",         
    "4" : "/u56db",         
    "5" : "/u4e94",         
    "6" : "/u516d"        
    };         
    if(/(Y+)/.test(fmt)){         
        fmt=fmt.replace(RegExp.$1, (this.getFullYear()+"").substr(4 - RegExp.$1.length));         
    }         
    if(/(E+)/.test(fmt)){         
        fmt=fmt.replace(RegExp.$1, ((RegExp.$1.length>1) ? (RegExp.$1.length>2 ? "/u661f/u671f" : "/u5468") : "")+week[this.getDay()+""]);         
    }         
    for(var k in o){         
        if(new RegExp("("+ k +")").test(fmt)){         
            fmt = fmt.replace(RegExp.$1, (RegExp.$1.length==1) ? (o[k]) : (("00"+ o[k]).substr((""+ o[k]).length)));         
        }         
    }         
    return fmt;         
} 

exports.setup = function (app, io) {
	exports.init();
	
	var combineTransferedData = function (codes, maincb) {
		async.eachSeries(codes, function(code, mapcb) {
			code = code.trim();
			//1, get all transfer basic data;
			//2, add sz index;
			//3, add all transfer advance data.
			async.waterfall([ function(cb) {
				db.collection(TRANSFER_BASIC_TABLE).find({"code" : code}).sort({"date" : -1}).toArray(function(err, result) {
					if (!err) {
						cb(null, result);
					}
				});

			}, function(data, cb) {
				async.map(data, function(obj, callback) {
					db.collection(TRANSFER_BASIC_TABLE).find({"code" : "sh000001", "date": obj.date}).toArray(function(err, result) {
						if (!err) {
							obj.szindex = result[0].ma;
							obj.szindex5 = result[0].ma5;
							obj.szindex10 = result[0].ma10;
							obj.szindex20 = result[0].ma20;
							obj.szindex30 = result[0].ma30;
							obj.szindex60 = result[0].ma60;
							obj.szindex90 = result[0].ma90;
							obj.szindex120 = result[0].ma120;
							
							obj.discreteszindex = getDiscrete(obj.szindex, obj.szindex5);
							obj.discreteszindex5 = getDiscrete(obj.szindex5, obj.szindex10);
							obj.discreteszindex10 = getDiscrete(obj.szindex10, obj.szindex20);
							obj.discreteszindex20 = getDiscrete(obj.szindex20, obj.szindex30);
							obj.discreteszindex30 = getDiscrete(obj.szindex30, obj.szindex60);
							obj.discreteszindex60 = getDiscrete(obj.szindex60, obj.szindex90);
							obj.discreteszindex90 = getDiscrete(obj.szindex90, obj.szindex120);
							
							delete obj.id;
							
							callback(null, obj);
						}
					});
				}, function(err, res) {
					if (!err) {
						cb(null, res);
					}
				});
				
			}, function(data, cb) {
				async.map(data, function(obj, callback) {
					db.collection(TRANSFER_ADVANCE_TABLE).find({"code" : obj.code, "date": obj.date}).toArray(function(err, result) {
						if (!err) {
							oldObj = result[0];
							delete oldObj.id;
							delete oldObj.code;
							delete oldObj.date;
							
							obj.date = new Date(obj.date).format('YYYY-MM-DD');
							extendObj(obj, oldObj);
							callback(null, obj);
						}
					});
				}, function(err, res) {
					if (!err) {
						cb(null, res);
					}
				});
			}, function(data, cb) {
				async.map(data, function(obj, callback) {
					var date = getAfterWorkingDay(obj.date, 1);
					db.collection(STOCK_BASIC_TABLE).find({"code" : obj.code, "date": date}).toArray(function(err, result) {
						if (!err) {
							if(result.length > 0) {
								res = result[0];
								if(res.changeRate > 0) {
									obj.positive = 1;
								}
								else {
									obj.positive = 0;
								}
							}
							callback(null, obj);
						}
					});
				}, function(err, res) {
					if (!err) {
						cb(null, res);
					}
				});
			} ], function(err, result) {
				if(!err) {
					if(result[0].code != '') {
						var data = {};
						data.fields = CSV_FIELDS;
						data.data = JSON.parse(JSON.stringify(result));
						var json2Csv = new SimpleJson2Csv(data);
						json2Csv.pipe(fs.createWriteStream(COMBINE_DATA_PATH + '/' + result[0].code + '.csv'));
					}
					mapcb();
				}
			});
		}, function(err) {
			console.log('Generate Data Task Finished!');
			io.emit('executor', 'Generate Data Task Finished!');
			maincb();
		});
		
	};
	
	app.get('/api/viewer/:code', function (req, res) {
		
		async.eachSeries(stockCodes, function(code, mapcb) {
			code = code.trim();
			//console.log(code);
			db.collection(TRANSFER_ADVANCE_TABLE).find({"code" : code}).sort({"date" : -1}).toArray(function(err, res) {
					result = res[0];
					//console.log(result.code + " " + result.ddx60 + " " + result.pddx60 + " " + result.fund60 + " " + result.pfund60 + " " + result.hide60 + " " + result.phide60);
					console.log(result.phide10);
					console.log(getDiscrete(result.phide10, 7));
					mapcb();
				});
				
		}, function(err) {
		});
		
		res.json({name: 1000});
		
	});
	
	var KDailyUrl = "http://api.finance.ifeng.com/index.php/akdaily/?code=";
	var KDailyType = "&type=fq";
	
	var KDailyAdvanceUrl = "http://data1.bestgo.com/stockdata/";
	var KDailyAdvancePost = "/kday.js?";
	
	var retrieveBasic = function(codes, cb) {
		async.eachSeries(codes, function(code, callback) {
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
			code = code.trim();
			var date = new Date().format('YYYY/MM/DD');
			var url = KDailyAdvanceUrl + code + KDailyAdvancePost + date;
			
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
			async.eachSeries(codes, function(code, callback2) {
				code = code.trim();
				var d = new Date(date).getTime();
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
			async.eachSeries(codes, function(code, callback2) {
				code = code.trim();
				var d = new Date(date).getTime();
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
		db.collection(STOCK_BASIC_TABLE).find({"code": code, "date": {$lte: date}}).sort({"date": -1}).toArray(function(err, result) {
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
				obj.ma90 = getDaysOfAveragePrice(stockList, 90);
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
				
				//4, calculate discrete for ma
				obj.discretema = getDiscrete(obj.ma, obj.ma5);
				obj.discretema5 = getDiscrete(obj.ma5, obj.ma10);
				obj.discretema10 = getDiscrete(obj.ma10, obj.ma20);
				obj.discretema20 = getDiscrete(obj.ma20, obj.ma30);
				obj.discretema30 = getDiscrete(obj.ma30, obj.ma60);
				obj.discretema60 = getDiscrete(obj.ma60, obj.ma90);
				obj.discretema90 = getDiscrete(obj.ma90, obj.ma120);
				
				//5, calculate discrete for hsl
				obj.discretehsl = getDiscrete(obj.hsl, obj.hsl5);
				obj.discretehsl5 = getDiscrete(obj.hsl5, obj.hsl10);
				obj.discretehsl10 = getDiscrete(obj.hsl10, obj.hsl20);
				obj.discretehsl20 = getDiscrete(obj.hsl20, obj.hsl30);
				obj.discretehsl30 = getDiscrete(obj.hsl30, obj.hsl60);
				obj.discretehsl60 = getDiscrete(obj.hsl60, obj.hsl90);
				obj.discretehsl90 = getDiscrete(obj.hsl90, obj.hsl120);
				
				//6, calculate discrete for up
				obj.discreteup = getDiscrete(obj.up, obj.up5);
				obj.discreteup5 = getDiscrete(obj.up5, obj.up10);
				obj.discreteup10 = getDiscrete(obj.up10, obj.up20);
				obj.discreteup20 = getDiscrete(obj.up20, obj.up30);
				obj.discreteup30 = getDiscrete(obj.up30, obj.up60);
				obj.discreteup60 = getDiscrete(obj.up60, obj.up90);
				obj.discreteup90 = getDiscrete(obj.up90, obj.up120);
				
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
				obj.ddx90 = getDaysOfDDX(stockList, 90);
				obj.ddx120 = getDaysOfDDX(stockList, 120);
				
				//2, calculate positive ddx
				obj.pddx = getDaysOfPositiveDDX(stockList, 1);
				obj.pddx5 = getDaysOfPositiveDDX(stockList, 5);
				obj.pddx10 = getDaysOfPositiveDDX(stockList, 10);
				obj.pddx20 = getDaysOfPositiveDDX(stockList, 20);
				obj.pddx30 = getDaysOfPositiveDDX(stockList, 30);
				obj.pddx60 = getDaysOfPositiveDDX(stockList, 60);
				obj.pddx90 = getDaysOfPositiveDDX(stockList, 90);
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
				
				//7, discrete for ddx
				obj.discreteddx = getDiscrete(obj.ddx, 0);
				obj.discreteddx5 = getDiscrete(obj.ddx5, 0);
				obj.discreteddx10 = getDiscrete(obj.ddx10, 0);
				obj.discreteddx20 = getDiscrete(obj.ddx20, 0);
				obj.discreteddx30 = getDiscrete(obj.ddx30, 0);
				obj.discreteddx60 = getDiscrete(obj.ddx60, 0);
				obj.discreteddx90 = getDiscrete(obj.ddx90, 0);
				obj.discreteddx120 = getDiscrete(obj.ddx120, 0);
				
				//8, discrete for pddx
				obj.discretepddx = getDiscrete(obj.ddx, 0);
				obj.discretepddx5 = getDiscrete(obj.pddx5, 3);
				obj.discretepddx10 = getDiscrete(obj.pddx10, 5);
				obj.discretepddx20 = getDiscrete(obj.pddx20, 10);
				obj.discretepddx30 = getDiscrete(obj.pddx30, 15);
				obj.discretepddx60 = getDiscrete(obj.pddx60, 30);
				obj.discretepddx90 = getDiscrete(obj.pddx90, 45);
				obj.discretepddx120 = getDiscrete(obj.pddx120, 60);
				
				//9, discrete for fund
				obj.discretefund = getDiscrete(obj.fund, 0);
				obj.discretefund5 = getDiscrete(obj.fund5, 0);
				obj.discretefund10 = getDiscrete(obj.fund10, 0);
				obj.discretefund20 = getDiscrete(obj.fund20, 0);
				obj.discretefund30 = getDiscrete(obj.fund30, 0);
				obj.discretefund60 = getDiscrete(obj.fund60, 0);
				obj.discretefund90 = getDiscrete(obj.fund90, 0);
				obj.discretefund120 = getDiscrete(obj.fund120, 0);
				
				//10, discrete for pfund
				obj.discretepfund = getDiscrete(obj.fund, 0);
				obj.discretepfund5 = getDiscrete(obj.pfund5, 3);
				obj.discretepfund10 = getDiscrete(obj.pfund10, 5);
				obj.discretepfund20 = getDiscrete(obj.pfund20, 10);
				obj.discretepfund30 = getDiscrete(obj.pfund30, 15);
				obj.discretepfund60 = getDiscrete(obj.pfund60, 30);
				obj.discretepfund90 = getDiscrete(obj.pfund90, 45);
				obj.discretepfund120 = getDiscrete(obj.pfund120, 60);
				
				//11, discrete for hide
				obj.discretehide = getDiscrete(obj.hide, 0);
				obj.discretehide5 = getDiscrete(obj.hide5, 0);
				obj.discretehide10 = getDiscrete(obj.hide10, 0);
				obj.discretehide20 = getDiscrete(obj.hide20, 0);
				obj.discretehide30 = getDiscrete(obj.hide30, 0);
				obj.discretehide60 = getDiscrete(obj.hide60, 0);
				obj.discretehide90 = getDiscrete(obj.hide90, 0);
				obj.discretehide120 = getDiscrete(obj.hide120, 0);
				
				//12, discrete for phide
				obj.discretephide = getDiscrete(obj.hide, 0);
				obj.discretephide5 = getDiscrete(obj.phide5, 3);
				obj.discretephide10 = getDiscrete(obj.phide10, 5);
				obj.discretephide20 = getDiscrete(obj.phide20, 10);
				obj.discretephide30 = getDiscrete(obj.phide30, 15);
				obj.discretephide60 = getDiscrete(obj.phide60, 30);
				obj.discretephide90 = getDiscrete(obj.phide90, 45);
				obj.discretephide120 = getDiscrete(obj.phide120, 60);
				
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
			stockObj.date = new Date(records[i][0]).getTime();
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
			
			db.collection(STOCK_BASIC_TABLE).insert(stockObj, {w:1}, function(err, result) {
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
			stockObj.date = new Date(records[i][0]).getTime();
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
				}, function(callback) {
					io.emit('executor', "Generating data task...");
					combineTransferedData(stockCodes, callback);
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
			else if(msg == 'generate_data'){
				io.emit('executor', "Generating data task...");
				combineTransferedData(stockCodes, function(){});
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
	
	var getDiscrete = function (a, b) {
		var res = 0;
		if(a > b) {
			res = 1;
		}
		else if(a == b) {
			res = 2;
		}
		else {
			res = 3;
		}
		return res;
	}
};
