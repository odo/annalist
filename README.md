annalist
=====

annalist is a statistics server written in Erlang.

It is made to hold to types of information:

*	Counts, as in number of events during a certain time period.
*	Timings, e.f. how long did requests take during a certain time period.

Installation
--------
make sure you have [rebar](https://github.com/basho/rebar) installed and in your path.

```git clone git@github.com:odo/annalist.git
cd annalist
make```

Starting
--------

start with default parameters by typing
```make start```

when you are using annalist from your own application, you can start it like this:

```annalist:start(DataDir, Host, Port, OutsidePort, Password, CompressThreshold, CompressFrequency).```

_DataDir_ is the Directory where the data is kept.
_Host_ is the host name used to call the API.
_Port_ is the port the HTTTP interface actually listerns to.
_OutsidePort_ is the port used to access the annalist from outside. _Port_ and _OutsidePort_ may differ when using port forwarding.
_CompressThreshold_ is the minimum number of data point before compression kicks in, 20 should be OK.
_CompressFrequency_ is the number of inserts before statistics data is compressed, 20 is a good number.


Usage: Counting
--------

Every event which is counted or timed has to be named. Names can be single binaries or a series of binaries organized like a path.

```[<<"read">>, <<"book">>]```

would mean an event of type "read", and more specifically an event of "read book".

so counting one "read book"-event:

```annalist_counter_server:count([<<"read">>, <<"book">>]).```

counts that event.

to get the counts back out, use:
```7> annalist_api_server:counts_with_labels([<<"read">>, <<"book">>], day, {2012, 07, 25}, 3). 
[{{2012,7,25},0},{{2012,7,26},1},{{2012,7,27},0}]```

So you said: Give me all counts for "read book" in the granularity "day", starting from 2012/07/25 for 3 days.

There is also ```annalist_api_server:counts/4``` tu return just the list of counts.

The granularities are 'second', 'minute', 'hour', 'day', 'month', 'year' and 'total'.

Usage: Timing
--------
In order to time things, use:
```annalist_recorder_server:record([<<"read">>, <<"book">>], 13.5).```

meaning the reading of a book took 13.5 ms.

To retriever values, use:
```1> annalist_api_server:quantiles_with_labels([<<"read">>, <<"book">>], day, {2012, 07, 25}, 3, 0.5).
[{{2012,7,25},undefined},
 {{2012,7,26},13.5},
 {{2012,7,27},undefined}]```

So as before, you ask for data of three days but this time for the median of the distribution (last argument been 0.5).
If you don't need the labels, use:

```> annalist_api_server:quantiles([<<"read">>, <<"book">>], day, {2012, 07, 25}, 3, 0.5).            
[undefined,13.5,undefined]```

Web interface
--------
Point your browser to http://host:port/annalist/sparks?tags=read%20book,eat%20apple
to see sparkline of the respective counts. Click on the labels to get a detailed graph.

Testing
--------

```make test```
