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
	erl -pz ebin deps/*/ebin -eval 'annalist:start("/tmp/annalist", "localhost", 8080, 8080, undefined).'

shell:
	erl -pz ebin deps/*/ebin

analyze: compile
	rebar analyze skip_deps=true

xref: compile
	rebar xref skip_deps=true