-module(store_riak_helper).
-author('Max Davidenko').

-behaviour(gen_server).

%% API
-export([start_link/1]).

-export([list_buckets/0, list_keys/1, get/2, get/3, put/1, put/2, delete/2, get_index/2]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, { riak_connection = null }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

list_buckets() ->
  gen_server:call(?SERVER, list_buckets, infinity).

list_keys(Bucket) ->
  gen_server:call(?SERVER, {list_keys, Bucket}, infinity).

get(Bucket, Key) ->
  gen_server:call(?SERVER, {get, Bucket, Key}, infinity).

get(Bucket, Key, Options) ->
  gen_server:call(?SERVER, {get, Bucket, Key, Options}, infinity).

put(Object) ->
  gen_server:call(?SERVER, {put, Object}, infinity).

put(Object, []) ->
  put(Object);
put(Object, Options) ->
  gen_server:call(?SERVER, {put, Object, Options}, infinity).

delete(Bucket, Key) ->
  gen_server:call(?SERVER, {delete, Bucket, Key}, infinity).

get_index(Bucket, Index) ->
  gen_server:call(?SERVER, {get_index, Bucket, Index}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(Args) ->
  Server = proplists:get_value(riak_server, Args, "localhost"),
  Port = proplists:get_value(riak_pb_port, Args, 8087),
  {ok, Pid} = riakc_pb_socket:start(Server, Port),
  {ok, #state{ riak_connection = Pid} }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call(list_buckets, _From, State) ->
  Reply = riakc_pb_socket:list_buckets(State#state.riak_connection),
  {reply, Reply, State};

handle_call({list_keys, Bucket}, _From, State) ->
  Reply = riakc_pb_socket:list_keys(State#state.riak_connection, Bucket),
  {reply, Reply, State};

handle_call({get, Bucket, Key}, _From, State) ->
  Reply = riakc_pb_socket:get(State#state.riak_connection, Bucket, Key),
  Res = case Reply of
    {ok, RawObj} ->
      {ok, decodeObj(riakc_obj:get_content_type(RawObj), riakc_obj:get_value(RawObj))};
    _ ->
      Reply
  end,
  {reply, Res, State};

handle_call({get, Bucket, Key, Options}, _From, State) ->
  Reply = riakc_pb_socket:get(State#state.riak_connection, Bucket, Key, Options),
  Res = case Reply of
    {ok, RawObj} ->
      {ok, decodeObj(riakc_obj:get_content_type(RawObj), riakc_obj:get_value(RawObj))};
    _ ->
      Reply
  end,
  {reply, Res, State};

handle_call({put, Object}, _From, State) ->
  Reply = riakc_pb_socket:put(State#state.riak_connection, Object),
  {reply, Reply, State};

handle_call({put, Object, Options}, _From, State) ->
  Reply = riakc_pb_socket:put(State#state.riak_connection, Object, Options),
  {reply, Reply, State};

handle_call({delete, Bucket, Key}, _From, State) ->
  Reply = riakc_pb_socket:delete(State#state.riak_connection, Bucket, Key),
  {reply, Reply, State};

handle_call({get_index, Bucket, {IndexId, IndexVal}}, _From, State) ->
  Reply = riakc_pb_socket:get_index_eq(State#state.riak_connection, Bucket, IndexId, IndexVal),
  {reply, Reply, State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

decodeObj(ContentType, Obj) ->
  case ContentType of
    "application/x-erlang-term" ->
      safeBinToTerm(Obj);
    "application/x-erlang-binary" ->
      safeBinToTerm(Obj);
    _ ->
      Obj
  end.

safeBinToTerm(Bin) ->
  try
    binary_to_term(Bin)
  catch
    _:Reason ->
      {error, Reason}
  end.


