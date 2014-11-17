-module(kvs_sup).
-behaviour(supervisor).
-export([start_link/0]).
-export([init/1]).
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(CHILD_WITH_ARGS(I, Type, Args), {I, {I, start_link, Args}, transient, 5000, Type, [I]}).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->

    kvs:start(),

    Backend = application:get_env(kvs, backend, mnesia),
    SupRes = case Backend of
      riak ->
        Server = application:get_env(kvs, riak_server, "localhost"),
        Port = application:get_env(kvs, riak_pb_port, 8087),
        Args = [{riak_server, Server}, {riak_pb_port, Port}],
        RiakHelper = ?CHILD_WITH_ARGS(store_riak_helper, worker, [Args]),
        RestartStrategy = {one_for_one, 5, 60},
        Childs = [RiakHelper],
        {ok, { RestartStrategy, Childs} };
      _ ->
        RestartStrategy = {one_for_one, 5, 60},
        Childs = [],
        {ok, { RestartStrategy, Childs} }
    end,
    SupRes.
