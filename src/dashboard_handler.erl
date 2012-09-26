-module(dashboard_handler).
-export([init/3, handle/2, terminate/2]).

init({tcp, http}, Req, _Opts) ->
    {ok, Req, {}}.

handle(Req, State) ->
    {ok, Req2} = cowboy_req:reply(200, [], page(), Req),
    {ok, Req2, State}.

terminate(_Req, _State) ->
    ok.

page() ->
	Host = proplists:get_value(host, application:get_all_env(annalist)),
	Port = proplists:get_value(outside_port, application:get_all_env(annalist)),
	PageRaw = list_to_binary(page_string()),
	PageHost = binary:replace(PageRaw, <<"$$HOST$$">>, list_to_binary(Host)),
	binary:replace(PageHost, <<"$$PORT$$">>, list_to_binary(integer_to_list(Port))).

page_string() ->
	"<html>
<head>
<script src='http://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js' type='text/javascript'></script>
<script src='http://www.highcharts.com/js/highstock.js' type='text/javascript'></script>
<script type='text/javascript'>

Host = '$$HOST$$';
Port = $$PORT$$;

function getUrlVars()
{
    var vars = [], hash;
    var hashes = window.location.href.slice(window.location.href.indexOf('?') + 1).split('&');
    for(var i = 0; i < hashes.length; i++)
    {
        hash = hashes[i].split('=');
        vars.push(hash[0]);
        vars[hash[0]] = hash[1];
    }
    return vars;
};

var Now = $.now();
var Second = 1000;
var Minute = Second * 60;
var Hour = Minute * 60;
var Day = Hour * 24;
var Month = Day * 30;
var Year = Month * 12;

var Plot;
var SecondPlot;
var MinutePlot;
var HourPlot;

(function($){ // encapsulate jQuery

Plot = function(ChartId, Title, URL, StartTime, Interval) {
	$.getJSON( URL + '?callback=?', function(data) {
		// Create the chart
		window.chart = new Highcharts.StockChart({
			chart : {
				renderTo : 'chart' + ChartId
			},
			title : {
				text : Title
			},
			series : [{
				dataGrouping : {approximation : 'sum'},
				animation: false,
				pointStart: Date.UTC(StartTime.getUTCFullYear(), StartTime.getUTCMonth(), StartTime.getUTCDate(), StartTime.getUTCHours(), StartTime.getUTCMinutes(), StartTime.getUTCSeconds()),
				pointInterval: Interval,
				data : data,
				tooltip: {
					yDecimals: 2
				}
			}]
		});
	})};
}


)(jQuery);

Tag = getUrlVars()['tag'];

SecondPlot = function(ChartId, Title, Host, Port, StartTime, ItemCount, Interval) {
	URL = 'http://' + Host + ':' + Port + '/annalist/second_counts/' + Tag + '/' +
		StartTime.getUTCFullYear() + '/' +
		(StartTime.getUTCMonth() + 1) + '/' +
		StartTime.getUTCDate() + '/' +
		StartTime.getUTCHours() + '/' +
		StartTime.getUTCMinutes() + '/' +
		(StartTime.getUTCSeconds() + 1) + '/' +
		ItemCount;
	Plot(ChartId, Title, URL, StartTime, Interval);
};

MinutePlot = function(ChartId, Title, Host, Port, StartTime, ItemCount, Interval) {
	URL = 'http://' + Host + ':' + Port + '/annalist/minute_counts/' + Tag + '/' +
		StartTime.getUTCFullYear() + '/' +
		(StartTime.getUTCMonth() + 1) + '/' +
		StartTime.getUTCDate() + '/' +
		StartTime.getUTCHours() + '/' +
		StartTime.getUTCMinutes() + '/' +
		ItemCount;
	Plot(ChartId, Title, URL, StartTime, Interval);
};

HourPlot = function(ChartId, Title, Host, Port, StartTime, ItemCount, Interval) {
	URL = 'http://' + Host + ':' + Port + '/annalist/hour_counts/' + Tag + '/' +
		StartTime.getUTCFullYear() + '/' +
		(StartTime.getUTCMonth() + 1) + '/' +
		StartTime.getUTCDate() + '/' +
		StartTime.getUTCHours() + '/' +
		ItemCount;
	Plot(ChartId, Title, URL, StartTime, Interval);
};

if (Tag == undefined) {alert('Please specify a tag in the URL (\"...dashboard?tag=my_tag\")')};
TagPrint = Tag.replace(/%20/g, '/');

OneHourAgo = new Date($.now() - Hour);
// SecondPlot(1, TagPrint + ' during Last Hour', Host, Port, OneHourAgo, Hour / Second, Second);
OneDayAgo = new Date($.now() - Day);
SecondPlot(2, TagPrint + ' during Last Day', Host, Port, OneDayAgo, Day / Second, Second);
OneMonthAgo = new Date($.now() - Month);
MinutePlot(3, TagPrint + ' during Last Month', Host, Port, OneMonthAgo, Month / Minute, Minute);
OneYearAgo = new Date($.now() - Year);
HourPlot(4, TagPrint + ' during Last Year', Host, Port
	, OneYearAgo, Year / Hour, Hour);

</script>


</script>
</head>
<body>
<!-- <div id='chart1' style='width: 800px; margin-left: 10px; float: left; height: 400px'></div> -->
<div id='chart2' style='width: 800px; margin-left: 10px; float: left; height: 400px'></div>
<div id='chart3' style='width: 800px; margin-left: 10px; float: left; height: 400px'></div>
<div id='chart4' style='width: 800px; margin-left: 10px; float: left; height: 400px'></div>
</html>".
