app: erl elixir

elixir:
	mix deps.get
	mix compile

erl: compile

compile clean eunit:
	rebar3 $@

test: eunit

.PHONY: test
