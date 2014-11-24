
$().ready(function () {
	var socket = io();
	socket.on('executor', function(msg) {
		$('#executor').append($('<p>').text(msg));
	});
	
	$('#retrieve').on('click', function() {
		var type = $('#retrieveType').val();
		// console.log(type);
		/*$.get("/api/executor/" + type, function(data) {
			alert("Data Loaded: " + data);
		});*/
		socket.emit('executor', type);
	});
  
});
