all: deps compile

compile:
	rebar compile

deps:
	rebar get-deps

clean:
	rebar clean

test:
	rebar skip_deps=true eunit

start:
	erl -pz ebin deps/*/ebin -eval 'annalist:start("/tmp/annlist", 8080).'

analyze: compile
	rebar analyze skip_deps=true