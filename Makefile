all: deps compile

compile:
	rebar compile skip_deps=true

deps:
	rebar get-deps

clean:
	rebar clean

test:
	TESTDIR=$(PWD)/private/ rebar skip_deps=true eunit

start:
	erl -pz ebin deps/*/ebin -eval 'annalist:start("/tmp/annlist", 8080).'

shell:
	erl -pz ebin deps/*/ebin

analyze: compile
	rebar analyze skip_deps=true