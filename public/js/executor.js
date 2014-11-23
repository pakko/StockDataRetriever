
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

  $('#visualize').on('click', function () {
    var checked = $('.viz-check:checked').attr('data-attr');
    var collectionName = $('#viz-collectionName').val();
    if (!collectionName)
      return alert('Please enter a collection name');

    var json = editor3.getSession().getValue();
    var query = JSON.parse(json);
    query.collection = collectionName;
    var $container = $('#vizcontainer');

    switch (checked) {
      case 'timeline':
        $container.Timeline({force: true, query: query});
        break;
      case 'metricbox':
        query.dimensions = [];
        $container.Metric({force: true, query: query});
        break;
      case 'table':
        $container.Table({force: true, query: query});
        break;
      case 'pie':
        $container.Pie({force: true, query: query});
        break;
      default:
        break;
    }
  });
  
});
