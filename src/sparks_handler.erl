-module(sparks_handler).
-export([init/3, handle/2, terminate/3]).

init({tcp, http}, Req, _Opts) ->
    {ok, Req, {}}.

handle(Req, State) ->
    {ok, Req2} = cowboy_req:reply(200, [], page(), Req),
    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.

page() ->
	Host = proplists:get_value(host, application:get_all_env(annalist)),
	Port = proplists:get_value(outside_port, application:get_all_env(annalist)),
	PageRaw = list_to_binary(page_string()),
	PageHost = binary:replace(PageRaw, <<"$$HOST$$">>, list_to_binary(Host)),
	binary:replace(PageHost, <<"$$PORT$$">>, list_to_binary(integer_to_list(Port))).

page_string() ->
"<!DOCTYPE HTML PUBLIC '-//W3C//DTD HTML 4.01//EN' 
    'http://www.w3.org/TR/html4/strict.dtd'>
<head>
    <style type='text/css'>
        .label1 {display:block; float:left; font-size: large; width:  350px;}
        .label2 {display:block; float:left; font-size: medium; width: 350px;}
        .label3 {display:block; float:left; font-size: small; width:  350px;}
        .label4 {display:block; float:left; font-size: small; width:  350px;}
        .label5 {display:block; float:left; font-size: small; width:  350px;}
        a {text-decoration: none; color: black;}
    </style>
    <script src='http://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js' type='text/javascript'></script>
    <script src='http://omnipotent.net/jquery.sparkline/2.0/jquery.sparkline.min.js' type='text/javascript'></script>
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

    Tags = getUrlVars()['tags'];
    if (Tags == undefined) {alert('Please specify a tag in the URL (\"...sparks?tags=tag1,tag2\")')};

    var Now = $.now();
    var Second = 1000;
    var Minute = Second * 60;
    var Hour = Minute * 60;
    var Day = Hour * 24;
    ThreeDaysAgo = new Date($.now() - 3 * Day);
    StartTime = ThreeDaysAgo;
    ItemCount =  3 * Day / Hour + 1;


    Plot = function(Index, Tag) {
        var TagPrint = Tag.replace(/%20/g, '/');
        var URL = 'http://' + Host + ':' + Port + '/annalist/hour_counts/' + Tag + '/' +
        StartTime.getUTCFullYear() + '/' +
        (StartTime.getUTCMonth() + 1) + '/' +
        StartTime.getUTCDate() + '/' +
        StartTime.getUTCHours() + '/' +
        ItemCount;
        
        var Class = 'chart_' + Index;
        var TotalID = 'total_' + Index;
        Nesting = TagPrint.split('/').length
        Html = '<div style=\"clear: left\"><span class=\"label'+ Nesting + '\"><a href=\"http://' + Host + ':' + Port + '/annalist/dashboard?tag=' + Tag + '\">' + TagPrint + ' (<span id=\"' + TotalID + '\"></span>)</a></span><span class=\"' + Class + '\">___________________________</span></div>';
        $('#sparks').append(Html);
        $.getJSON( URL + '?callback=?', function(data) {
            // Create the chart
            $('.' + Class).sparkline(data, {fillColor: 'white', lineColor: 'gray'});
        });
        var URLTotal = 'http://' + Host + ':' + Port + '/annalist/totals/' + Tag
        $.getJSON( URLTotal + '?callback=?', function(data) {
            console.log($('#' + TotalID));
            console.log(data + '');
            $('#' + TotalID).append('total: ' + data);
        });
    };

    $(function() {
        $.each(Tags.split(','), function(index, tag) { 
            Plot(index, tag); 
        });
    });
    </script>
</head>
<body>

<div><h2>Last 3 Days, one point per hour.</h1></div><p/>
<div id='sparks'>
</div>


</body>
</html>".
