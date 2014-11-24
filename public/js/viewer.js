$().ready(function () {
	$.get("/api/viewer/" + type, function(data) {
		alert("Data Loaded: " + data);
	});
  
});