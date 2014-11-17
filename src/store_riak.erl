-module(store_riak).
-author('Maxim Sokhatsky <maxim@synrc.com>').
-copyright('Synrc Research Center s.r.o.').
-include("config.hrl").
-include("user.hrl").
-include("subscription.hrl").
-include("group.hrl").
-include("comment.hrl").
-include("entry.hrl").
-include("feed.hrl").
-include("acl.hrl").
-compile(export_all).

-define(RIAK_BACKEND, store_riak_helper).

start() -> ok.
stop() -> ok.
version() -> {version,"KVS RIAK 2.0.2"}.
join() -> initialize(), ok.
join(_) -> initialize(), ok.
initialize() -> ok.

dir() ->
    {ok,Buckets} = ?RIAK_BACKEND:list_buckets(),
    [{table,binary_to_list(X)}||X<-Buckets].

riak_clean(Table) when is_list(Table)->
    {ok,Keys}=?RIAK_BACKEND:list_keys(erlang:list_to_binary(Table)),
    [ ?RIAK_BACKEND:delete(erlang:list_to_binary(Table),Key) || Key <- Keys];
riak_clean(Table) ->
    [TableStr] = io_lib:format("~p",[Table]),
    {ok,Keys}=?RIAK_BACKEND:list_keys(erlang:list_to_binary(TableStr)),
    [ kvs:delete(Table,key_to_bin(Key)) || Key <- Keys].

make_object(T) ->
    Bucket = element(1,T),
    Key = element(2,T),
    Obj1 = riakc_obj:new(key_to_bin(Bucket), key_to_bin(Key), T),
    Indices = make_indices(T),
    Meta = dict:store(<<"index">>, Indices, dict:new()),
    Obj2 = riakc_obj:update_metadata(Obj1, Meta),
    error_logger:info_msg("RIAK PUT IDX ~p",[Indices]),
    Obj2.

make_indices(#subscription{who=Who, whom=Whom}) -> [
    {<<"who_bin">>, key_to_bin(Who)},
    {<<"whom_bin">>, key_to_bin(Whom)}];

make_indices(#user{id=UId,zone=Zone}) -> [
    {<<"user_bin">>, key_to_bin(UId)},
    {<<"zone_bin">>, key_to_bin(Zone)}];

make_indices(#comment{id={CID,EID},from=Who}) -> [
    {<<"comment_bin">>, key_to_bin({CID,EID})},
    {<<"author_bin">>, key_to_bin(Who)}];

make_indices(#entry{id={EID,FID},entry_id=EntryId,feed_id=Feed,from=From,to=To}) -> [
    {<<"entry_feed_bin">>, key_to_bin({EID,FID})},
    {<<"entry_bin">>, key_to_bin(EntryId)},
    {<<"from_bin">>, key_to_bin(From)},
    {<<"to_bin">>, key_to_bin(To)},
    {<<"feed_bin">>, key_to_bin(Feed)}];

make_indices(Record) -> [
    {key_to_bin(atom_to_list(element(1,Record))++"_bin"),key_to_bin(element(2,Record))}].

put(Records) when is_list(Records) -> lists:foreach(fun riak_put/1, Records);
put(Record) -> riak_put(Record).

riak_put(Record) ->
    Object = make_object(Record),
    Result = ?RIAK_BACKEND:put(Object),
    Result.

put_if_none_match(Record) ->
    Object = make_object(Record),
    case ?RIAK_BACKEND:put(Object, [if_none_match]) of
        ok -> ok;
        Error -> Error end.

update(Record, Object) ->
    NewObject = make_object(Record),
    NewKey = riakc_obj:key(NewObject),
    case riakc_obj:key(Object) of
        NewKey ->
            MetaInfo = riakc_obj:get_update_metadata(NewObject),
            UpdObject2 = riakc_obj:update_value(Object, Record),
            UpdObject3 = riakc_obj:update_metadata(UpdObject2, MetaInfo),
            case ?RIAK_BACKEND:put(UpdObject3, [if_not_modified]) of
                ok -> ok;
                Error -> Error
            end;
        _ -> {error, keys_not_equal}
    end.

get(Tab, Key) ->
    Bucket = key_to_bin(Tab),
    IntKey = key_to_bin(Key),
    riak_get(Bucket, IntKey).

riak_get(Bucket,Key) ->
    ?RIAK_BACKEND:get(Bucket,Key).

get_for_update(Tab, Key) ->
    case ?RIAK_BACKEND:get(key_to_bin(Tab), key_to_bin(Key)) of
        {ok, O} -> {ok, riakc_obj:get_value(O), O};
        Error -> Error end.

delete(Tab, Key) ->
    Bucket = key_to_bin(Tab),
    IntKey = key_to_bin(Key),
    ?RIAK_BACKEND:delete(Bucket, IntKey).

delete_by_index(Tab, IndexId, IndexVal) ->
    Bucket = key_to_bin(Tab),
    {ok, Keys} = ?RIAK_BACKEND:get_index(Bucket, {IndexId, key_to_bin(IndexVal)}),
    [?RIAK_BACKEND:delete(Bucket, Key) || Key <- Keys].

key_to_bin(Key) ->
    if is_integer(Key) -> erlang:list_to_binary(integer_to_list(Key));
       is_list(Key) -> erlang:list_to_binary(Key);
       is_atom(Key) -> erlang:list_to_binary(erlang:atom_to_list(Key));
       is_binary(Key) -> Key;
       true ->  [ListKey] = io_lib:format("~p", [Key]), erlang:list_to_binary(ListKey) end.

all(RecordName) ->
    RecordBin = key_to_bin(RecordName),
    {ok,Keys} = ?RIAK_BACKEND:list_keys(RecordBin),
    Results = [ riak_get_raw({RecordBin, Key, riak_client}) || Key <- Keys ],
    [ Object || Object <- Results, Object =/= failure ].

all_by_index(Tab, IndexId, IndexVal) ->
    Bucket = key_to_bin(Tab),
    {ok, Keys} = ?RIAK_BACKEND:get_index(Bucket, {IndexId, key_to_bin(IndexVal)}),
    lists:foldl(fun(Key, Acc) ->
        case ?RIAK_BACKEND:get(Bucket, Key, []) of
            {ok, O} -> [riakc_obj:get_value(O) | Acc];
            {error, notfound} -> Acc end end, [], Keys).

riak_get_raw({RecordBin, Key, _Riak}) ->
    case ?RIAK_BACKEND:get(RecordBin, Key) of
        {ok,O} -> riakc_obj:get_value(O);
        _ -> failure end.

next_id(CounterId) -> next_id(CounterId, 1).
next_id(CounterId, Incr) -> next_id(CounterId, 0, Incr).
next_id(CounterId, Default, Incr) ->
    CounterBin = key_to_bin(CounterId),
    {Object, Value, Options} =
        case ?RIAK_BACKEND:get(key_to_bin(id_seq), CounterBin, []) of
            {ok, CurObj} ->
                R = #id_seq{id = CurVal} = riakc_obj:get_value(CurObj),
                NewVal = CurVal + Incr,
                Obj = riakc_obj:update_value(CurObj, R#id_seq{id = NewVal}),
                {Obj, NewVal, [if_not_modified]};
            {error, notfound} ->
                NewVal = Default + Incr,
                Obj = riakc_obj:new(key_to_bin(id_seq), CounterBin, #id_seq{thing = CounterId, id = NewVal}),
                {Obj, NewVal, [if_none_match]} end,
    case ?RIAK_BACKEND:put(Object, Options) of
        ok -> Value;
        {error, _} -> next_id(CounterId, Incr) end.

% index funs

subscriptions(UId) -> all_by_index(subsciption, <<"subs_who_bin">>, list_to_binary(UId)).
subscribed(Who) -> all_by_index(subscription, <<"subs_whom_bin">>, list_to_binary(Who)).
author_comments(Who) ->
    EIDs = [E || #comment{entry_id=E} <- all_by_index(comment,<<"author_bin">>, Who) ],
    lists:flatten([ all_by_index(entry,<<"entry_bin">>,EID) || EID <- EIDs]).
