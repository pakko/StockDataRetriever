var express = require('express'), 
	path = require('path'), 
	http = require('http'), 
	router = require('./routes'), 
	api = require('./routes/api');

var app = express();

app.set('view engine', 'jade');
app.configure(function() {
	app.set('port', process.env.PORT || 3000);
	app.use(express.logger('dev')); /* 'default', 'short', 'tiny', 'dev' */
	app.use(express.bodyParser());
	app.use(express.static(path.join(__dirname, 'public')));
});

var server = http.Server(app);
var io = require('socket.io')(server);

router.setup(app);
api.setup(app, io);

server.listen(app.get('port'), function() {
	console.log("Express server listening on port " + app.get('port'));
});
