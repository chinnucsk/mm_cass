-module(cassanderl).

-behaviour(gen_server).

%% API
-export([start_link/1, test/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {conn}).
-define(SERVER, ?MODULE).

-include("../../deps/cassandra_thrift/include/cassandra_types.hrl").

%%====================================================================
%% API
%%====================================================================

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Open Connection
    {ok, Params} = config:get(cassandra),
    Hostname     = proplists:get_value(hostname, Params),
    Port         = proplists:get_value(port, Params),
    KeySpace     = proplists:get_value(keyspace, Params),

    {ok, Conn} = thrift_client_util:new(Hostname, Port, cassandra_thrift, [{framed, true}]), ok,
    log:info("Connection to cassandra: Ok;"),
    %% Set KeySpace
    {Conn2, {ok, ok}} = thrift_client:call(Conn, set_keyspace, [KeySpace]),
    {ok, #state{conn = Conn2}}.

handle_call({call, Function, Args}, _From, #state{conn=Conn}=State) ->
    try thrift_client:call(Conn, Function, Args) of
        {NewConn, Response} ->
            NewState = State#state{conn=NewConn},
            {reply, {ok, Response}, NewState}
    catch
        {NewConn, {exception, {Exception}}} ->
            NewState = State#state{conn=NewConn},
            {reply, {exception, Exception}, NewState}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    thrift_client:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


test()->
    %% Open Connection
    {ok, Params} = config:get(cassandra),
    Hostname = proplists:get_value(hostname, Params),
    Port     = proplists:get_value(port, Params),
    KeySpace     = proplists:get_value(keyspace, Params),
    {ok, Conn} = thrift_client_util:new(Hostname, Port, cassandra_thrift, [{framed, true}]),

    %% Set KeySpace
    {Conn2, {ok, ok}} = thrift_client:call(Conn, set_keyspace, [KeySpace]),

    %thrift_client:call(Conn2, 'system_drop_column_family', [#cfDef{keyspace=KeySpace, name="mycf"}]),

    Res0 = thrift_client:call(Conn2, 'system_add_column_family', 
        [
            #cfDef{keyspace=KeySpace, name="mycf"}
        ]
        ),
    Key = "00000001",
    {Conn3, {ok,ok}} = thrift_client:call(Conn2, 'insert', 
        [
            Key, 
            #columnParent{column_family="mycf"}, 
            #column{name="col1", value="Hello World !", timestamp=0}, 
            ?cassandra_ConsistencyLevel_ONE
        ]
    ),
    %{Conn3, {ok,ok}} = thrift_client:call(Conn2, 'insert', 
    %    [Key, 
    %        #columnParent{column_family="mycf"}, 
    %        #column{name="col1", value="Hello World !", timestamp=0}, 
    %        ?cassandra_ConsistencyLevel_ONE]),
    try 
        thrift_client:call(Conn3,'get',
            [Key,  
             #columnPath{column_family="mycf", 
             %super_column="col_A", 
             column = "col1"}, 
             ?cassandra_ConsistencyLevel_ONE]
        )  of
        {_C1,{ok,Val}}  -> io:format("Getted: ~p",[Val])
        catch   
            { _, {exception, {notFoundException} = Err}} -> io:format("here err")
        end,
    ok.