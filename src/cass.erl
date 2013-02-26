%%  Copyright (C) 2011 - Molchanov Maxim,
%% 
%% @author Maxim Molchanov <elzor.job@gmail.com>
%% @reference See <a href="http://wiki.apache.org/cassandra/API">Cassandra API</a>.
%% @version:  {@version}

-module(cass).

-export([
		 column_parent/1, 
		 column_parent/2, 
		 slice/2, 
		 slice/4,
		 slice/5,
		 set_keyspace/1, 
		 get/4, 
		 get/5, 
         insert/6,
         insert/7,
		 insert/8, 
		 add/4,
		 get_slice/4,
		 multiget_slice/4,
		 remove/5,
		 truncate/1,
		 describe_cluster_name/0,
		 describe_schema_versions/0,
		 describe_keyspace/1,
		 describe_partitioner/0,
		 describe_ring/1,
		 describe_version/0,
		 system_add_column_family/2,
         system_add_column_family/3,
         system_drop_column_family/2,
         system_add_keyspace/2,
         system_add_keyspace/3,
         system_drop_keyspace/1,
         system_update_keyspace/2,
         system_update_keyspace/3,
         system_update_column_family/2,
         get_count/5,
         multiget_count/5,
         test_verbose/0,
         pre_tests/0
		]
).

-include("../../deps/cassandra_thrift/include/cassandra_types.hrl").

%% ----------------------------------
%% API
%% ----------------------------------

-spec column_parent(Family::string())-> 
    Record::#columnParent{ 
        column_family::string() 
    }.
column_parent(Family) ->
    #columnParent { column_family = Family }.

-spec column_parent(Super::string(), Family::string())->
    Record::#columnParent{
        super_column::string(),
        column_family::string()
    }.
column_parent(Super, Family) ->
    #columnParent { super_column = Super,
                    column_family = Family }.

%% @doc Create a slice_range object for slice queries
%% @end
-spec slice(Start::integer(), End::integer())->
    Record::#slicePredicate{ 
        slice_range::#sliceRange{
            start::integer() | any(),
            finish::integer() | any(),
            count::integer()
        },
        column_names::atom()
    }.
slice(Start, End) ->
    R = #sliceRange { start = Start, finish = End, count = 1000 },
    #slicePredicate { slice_range = R,
                      column_names = undefined }.

-spec slice(Start::integer(), End::integer(), Reversed::boolean(), Count::integer())->
    Record::#slicePredicate{
        slice_range::#sliceRange{
            start::integer() | any(),
            finish::integer() | any(),
            count::integer()
        },
        column_names::atom()
    }.
slice(Start, End, Reversed, Count)->
	R = #sliceRange { start = Start, finish = End, reversed = Reversed, count = Count },
	#slicePredicate { slice_range = R,
                      column_names = undefined }.

-spec slice(ColumnNames::list(), Start::integer(), End::integer(), Reversed::boolean(), Count::integer())->
    Record::#slicePredicate{
        slice_range::#sliceRange{
            start::integer() | any(),
            finish::integer() | any(),
            count::integer()
        },
        column_names::list()
    }.
slice(ColumnNames, Start, End, Reversed, Count)->
	R = #sliceRange { start = Start, finish = End, reversed = Reversed, count = Count },
	#slicePredicate { slice_range = R,
                      column_names = ColumnNames }.

%% @doc Set the keyspace to use for subsequent requests. 
%% Throws InvalidRequestException for an unknown keyspace.
-spec set_keyspace(Keyspace::string())-> 
    tuple().
set_keyspace(Keyspace) ->
    cass_sup:call(set_keyspace, [Keyspace]).

%% @doc Get the Column or SuperColumn at the given column_path.
-spec get(Key::string(), ColumnFamily::string(), Column::string(), ConsistencyLevel::integer()) -> 
	{ok,{ok, Res::tuple()}} 
	| {exception,notFoundException}.
get(Key, ColumnFamily, Column, ConsistencyLevel)->
	ColumnPath = #columnPath {
        column_family = ColumnFamily,
        column = Column
    },
    cass_sup:call(get, [Key, ColumnPath, ConsistencyLevel]).

-spec get(Key::string(), ColumnFamily::string(),  SuperColumn::string(), Column::string(), ConsistencyLevel::integer()) -> 
	{ok,{ok, Res::tuple()}} 
	| {exception,notFoundException}.
get(Key, ColumnFamily, SuperColumn, Column, ConsistencyLevel) ->
    ColumnPath = #columnPath {
        column_family = ColumnFamily,
        super_column = SuperColumn,
        column = Column
    },
    cass_sup:call(get, [Key, ColumnPath, ConsistencyLevel]).

%% @doc Get the group of columns contained by column_parent 
%% (either a ColumnFamily name or a ColumnFamily/SuperColumn name pair) 
%% specified by the given SlicePredicate struct.
%% @end
-spec get_slice(Key::string(), ColumnParent::#columnParent{}, Slice::any(), ConsistencyLevel::integer())-> 
    tuple().
get_slice(Key, ColumnParent, Slice, ConsistencyLevel)->
	cass_sup:call(get_slice, [Key, ColumnParent, Slice, ConsistencyLevel]). 

%% @doc Retrieves slices for column_parent and predicate on each of 
%% the given keys in parallel. Keys are a list of strings of the keys to get slices for.
%% @end
-spec multiget_slice(KeyList::list(), ColumnParent::#columnParent{}, Slice::any(), ConsistencyLevel::integer())->
    tuple().
multiget_slice(KeyList, ColumnParent, Slice, ConsistencyLevel) ->
	cass_sup:call(multiget_slice, [KeyList, ColumnParent, Slice, ConsistencyLevel]).	

%% @doc Insert a Column consisting of (name, value, timestamp, ttl) at the given ColumnParent.
%% ConsistemcyLevel may be:
%%   - cassandra_ConsistencyLevel_ONE
%%   - cassandra_ConsistencyLevel_TWO
%%   - cassandra_ConsistencyLevel_THREE
%%   - cassandra_ConsistencyLevel_QUORUM
%%   - cassandra_ConsistencyLevel_LOCAL_QUORUM
%%   - cassandra_ConsistencyLevel_EACH_QUORUM
%%   - cassandra_ConsistencyLevel_ALL
%%   - cassandra_ConsistencyLevel_ANY
%% @end
-spec insert(
        Key::string(), 
        ColumnFamily::string(), 
        SuperColumn::string(), 
        Name::string(), 
        Value::any(), 
        Timestamp::integer(), 
        Ttl::integer(), 
        ConsistencyLevel::integer()
    ) -> tuple().
insert(Key, ColumnFamily, SuperColumn, Name, Value, Timestamp, Ttl, ConsistencyLevel) ->
    ColumnParent = #columnParent {
        column_family = ColumnFamily,
        super_column = SuperColumn  
    },
    Column = #column {
        name = Name,
        value = Value,
        timestamp = Timestamp,
        ttl = Ttl
    },
    cass_sup:call(insert, [Key, ColumnParent, Column, ConsistencyLevel]).

insert(Key, ColumnFamily, Name, Value, Timestamp, Ttl, ConsistencyLevel) ->
    ColumnParent = #columnParent {
        column_family = ColumnFamily
    },
    Column = #column {
        name = Name,
        value = Value,
        timestamp = Timestamp,
        ttl = Ttl
    },
    cass_sup:call(insert, [Key, ColumnParent, Column, ConsistencyLevel]).

insert(Key, ColumnFamily, Name, Value, Timestamp, ConsistencyLevel) ->
    ColumnParent = #columnParent {
        column_family = ColumnFamily
    },
    Column = #column {
        name = Name,
        value = Value,
        timestamp = Timestamp
    },
    cass_sup:call(insert, [Key, ColumnParent, Column, ConsistencyLevel]).

%% @doc Increments a CounterColumn consisting of (name, value) 
%% at the given ColumnParent.
%% @end
-spec add (
        Key::string(), 
        ColumnParent::#columnParent{} | #counterColumn{}, 
        Nv::tuple() | {Name::string(), Value::any()}, 
        ConsistencyLevel::integer()
    ) -> tuple().
add(Key, ColumnParent, {Name, Value}, ConsistencyLevel) ->
    add(Key, ColumnParent, #counterColumn {
      name = Name,
      value = Value }, ConsistencyLevel);
add(Key, ColumnParent, #counterColumn{} = Col, ConsistencyLevel) ->
    cass_sup:call(add, [Key, ColumnParent, Col, ConsistencyLevel]).

%% @doc Remove data from the row specified by key at the granularity
%% specified by column_path, and the given timestamp.
%% @end
-spec remove(
        Key::string(), 
        ColumnFamily::string(), 
        SuperColumn::string(), 
        Column::string(), 
        ConsistencyLevel::integer()
    ) -> tuple().
remove(Key, ColumnFamily, SuperColumn, Column, ConsistencyLevel ) ->
	ColumnPath = #columnPath {
        column_family = ColumnFamily,
        super_column = SuperColumn,
        column = Column
    },
    cass_sup:call(remove, [Key, ColumnPath, utils:unix_timestamp(), ConsistencyLevel]).

-spec get_count(
        Key::string(), 
        ColumnFamily::string(), 
        SuperColumn::string(), 
        Slice::#slicePredicate{}, 
        ConsistencyLevel::integer()
    ) -> tuple().
get_count(Key, ColumnFamily, SuperColumn, Slice, ConsistencyLevel)->
	ColumnParent = #columnParent {
        column_family = ColumnFamily,
        super_column = SuperColumn
    },
    cass_sup:call(get_count, [Key, ColumnParent, Slice, ConsistencyLevel]).

-spec multiget_count(
        Keys::list(), 
        ColumnFamily::string(), 
        SuperColumn::string(), 
        Slice::#slicePredicate{}, 
        ConsistencyLevel::integer()
    ) -> tuple().
multiget_count(Keys, ColumnFamily, SuperColumn, Slice, ConsistencyLevel)->
	ColumnParent = #columnParent {
        column_family = ColumnFamily,
        super_column = SuperColumn
    },
    cass_sup:call(multiget_count, [Keys, ColumnParent, Slice, ConsistencyLevel]).	

% @doc Removes all the rows from the given column family.
-spec truncate(ColumnFamily::string()) -> 
    tuple().
truncate(ColumnFamily)->
	cass_sup:call(truncate, [ColumnFamily]).	

% @doc Gets the name of the cluster.
-spec describe_cluster_name()->tuple().
describe_cluster_name()->
	cass_sup:call(describe_cluster_name, []).	

%% @doc For each schema version present in the cluster,
%% returns a list of nodes at that version.
%% Hosts that do not respond will be under the key DatabaseDescriptor.INITIAL_VERSION. 
%% The cluster is all on the same version if the size of the map is 1.
%% @end
-spec describe_schema_versions()-> tuple().
describe_schema_versions()->
	cass_sup:call(describe_schema_versions, []).	

%% @doc Gets information about the specified keyspace.
-spec describe_keyspace(Keyspace::string()) -> tuple().
describe_keyspace(Keyspace)->
	cass_sup:call(describe_keyspace, [Keyspace]).		

%% @doc Gets the name of the partitioner for the cluster.
-spec describe_partitioner() -> tuple().
describe_partitioner()->
	cass_sup:call(describe_partitioner, []).

%% @doc Gets the token ring; a map of ranges to host addresses.
-spec describe_ring(Keyspace::string())-> tuple().
describe_ring(Keyspace)->
	cass_sup:call('describe_ring', [Keyspace]).	

%% @doc Gets the Thrift API version.
-spec describe_version()->tuple().
describe_version()->
	cass_sup:call('describe_version', []).		

%% @doc Adds a column family.
-spec system_add_column_family(Keyspace::string(), Name::string())->tuple().
system_add_column_family(Keyspace, Name)->
	cass_sup:call('system_add_column_family', [
        #cfDef{keyspace=Keyspace, name=Name}]
    ).

system_add_column_family(Keyspace, Name, ColumnType)->
    cass_sup:call('system_add_column_family', [
        #cfDef{keyspace=Keyspace, name=Name, column_type=ColumnType}]
    ).

%% @doc Drops a column family.
-spec system_drop_column_family(Keyspace::string(), Name::string())->tuple().
system_drop_column_family(Keyspace, Name)->
	cass_sup:call('system_drop_column_family', [#cfDef{keyspace=Keyspace, name=Name}]).	

%% @doc Creates a new keyspace and any column families defined with it.
%% StrategyClass may be:
%%  - SimpleStrategy (for simple single data center clusters)
%%  - NetworkTopologyStrategy (for multiple data centers)
%%    * Two replicas in each data center if we use consistency level of ONE
%%    * Three replicas in each data center if we use consistency level of LOCAL_QUORUM
%% @reference See http://www.datastax.com/docs/1.0/cluster_architecture/replication#replication-strategy
-spec system_add_keyspace(Name::string(), StrategyClass::string())->tuple().
system_add_keyspace(Name, StrategyClass)->
    system_add_keyspace(Name, StrategyClass, 2).
-spec system_add_keyspace(Name::string(), StrategyClass::string(), ReplicationFactor::integer())->tuple().
system_add_keyspace(Name, StrategyClass, ReplicationFactor)->
    cass_sup:call('system_add_keyspace', [
        #ksDef{name=Name, strategy_class=StrategyClass, replication_factor=ReplicationFactor, cf_defs=[]}
    ]).     

%% @doc Drops a keyspace.
-spec system_drop_keyspace(Name::string())->tuple().
system_drop_keyspace(Name)->
    cass_sup:call('system_drop_keyspace', [Name]).

%% @doc Updates properties of a keyspace. returns the new schema id.
system_update_keyspace(Name, StrategyClass)->
    system_update_keyspace(Name, StrategyClass, 2).
-spec system_update_keyspace(Name::string(), StrategyClass::string(), ReplicationFactor::integer())->tuple().
system_update_keyspace(Name, StrategyClass, ReplicationFactor)->
    cass_sup:call('system_update_keyspace', [
        #ksDef{name=Name, strategy_class=StrategyClass, replication_factor=ReplicationFactor, cf_defs=[]}
    ]).         

-spec system_update_column_family(Keyspace::string(), Name::string())->tuple().
system_update_column_family(Keyspace, Name)->
    cass_sup:call('system_update_column_family', [
            #cfDef{keyspace=Keyspace, name=Name}]
    ).

%% ----------------------------------
%% Tests
%% ----------------------------------

%%-ifdef(EUNIT).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_KEY_SPACE,    "tksp").
-define(TEST_COLUMNFAMILY, "tcf").

clear_test_arrea()->
    try
        system_drop_keyspace(?TEST_KEY_SPACE)    
    catch
        _:_->
            pass 
    end. 

pre_tests()->
    clear_test_arrea().

test_verbose()->
    eunit:test(optimecass, [verbose]).


a1_test()->
    catch pre_tests(),
    ok.

system_add_keyspace_single_and_describe_keyspace_and_drop_keyspace_test()->
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    set_keyspace(?TEST_KEY_SPACE),
    {ok,{ok,{ksDef,_,<<"org.apache.cassandra.locator.SimpleStrategy">>,_,2,_,_}}} = describe_keyspace(?TEST_KEY_SPACE),
    
    system_drop_keyspace(?TEST_KEY_SPACE), 
    {exception,notFoundException} = describe_keyspace(?TEST_KEY_SPACE),
    ok. 

system_update_keyspace_test()->    
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    set_keyspace(?TEST_KEY_SPACE),
    {ok,{ok,{ksDef,_,<<"org.apache.cassandra.locator.SimpleStrategy">>,_,2,_,_}}} = describe_keyspace(?TEST_KEY_SPACE),
    system_update_keyspace(?TEST_KEY_SPACE, "SimpleStrategy",3),
    {ok,{ok,{ksDef,_,<<"org.apache.cassandra.locator.SimpleStrategy">>,_,3,_,_}}} = describe_keyspace(?TEST_KEY_SPACE),
    
    system_drop_keyspace(?TEST_KEY_SPACE), 
    {exception,notFoundException} = describe_keyspace(?TEST_KEY_SPACE),
    ok.

truncate_test()->
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    set_keyspace(?TEST_KEY_SPACE),
    {ok, {ok, Addres}} = system_add_column_family(?TEST_KEY_SPACE, ?TEST_COLUMNFAMILY),
    ?assert(is_binary(Addres)=:=true),
    set_keyspace(?TEST_KEY_SPACE),
    {ok,{ok, ok}} = truncate(?TEST_COLUMNFAMILY), 
    
    system_drop_keyspace(?TEST_KEY_SPACE), 
    {exception,notFoundException} = describe_keyspace(?TEST_KEY_SPACE),
    ok. 

system_add_column_family_test()-> 
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    set_keyspace(?TEST_KEY_SPACE),
    {ok,{ok,{ksDef,_,<<"org.apache.cassandra.locator.SimpleStrategy">>,_,2,_,_}}} = describe_keyspace(?TEST_KEY_SPACE),

    {ok, {ok, Addres}} = system_add_column_family(?TEST_KEY_SPACE, ?TEST_COLUMNFAMILY),
    ?assert(is_binary(Addres)=:=true),  

    
    system_drop_keyspace(?TEST_KEY_SPACE), 
    {exception,notFoundException} = describe_keyspace(?TEST_KEY_SPACE), 
    ok. 


column_parent1_test()->
    Name = column_parent("Test"),
    ?assert(is_list(Name#columnParent.column_family)=:=true),
    ?assert(Name#columnParent.column_family/=[]),
    ok.


column_parent2_test()->
    Name = column_parent("Test", "Test"),
    ?assert(is_list(Name#columnParent.column_family)=:=true),
    ?assert(is_list(Name#columnParent.super_column)=:=true),
    ?assert(Name#columnParent.column_family/=[]),
    ?assert(Name#columnParent.super_column/=[]), 
    ok.

describe_cluster_name_test()->
    {ok, {ok,Name}} = describe_cluster_name(),
    ?assert(is_binary(Name)=:=true),
    ok.

describe_version_test()->
    {ok, {ok,Name}} = describe_version(),
    ?assert(is_binary(Name)=:=true),
    ok.    

describe_partitioner_test()->
    {ok, {ok,Name}} = describe_partitioner(),
    ?assert(is_binary(Name)=:=true),
    ok.  

describe_schema_versions_test()->
    {ok, {ok,Name}} = describe_schema_versions(),
    ?assert(is_tuple(Name)=:=true),
    ok.          

describe_ring_test()->
    ok.

insert8_test()-> %% SuperColumn test
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    system_add_column_family(?TEST_KEY_SPACE, ?TEST_COLUMNFAMILY, "Super"),
    set_keyspace(?TEST_KEY_SPACE),
    insert(
        "00001", 
        ?TEST_COLUMNFAMILY,
        "tsc", 
        "col1_test", 
        "test value, test, text, test", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE 
    ),
    
    system_drop_keyspace(?TEST_KEY_SPACE),
    ok.


insert7_test()->
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    system_add_column_family(?TEST_KEY_SPACE, ?TEST_COLUMNFAMILY),
    set_keyspace(?TEST_KEY_SPACE),
    insert(
        "00001", 
        ?TEST_COLUMNFAMILY, 
        "col1_test", 
        "test value, test, text, test", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),
    
    system_drop_keyspace(?TEST_KEY_SPACE),
    ok.


insert6_urlkey_test()-> 
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    system_add_column_family(?TEST_KEY_SPACE, ?TEST_COLUMNFAMILY),
    set_keyspace(?TEST_KEY_SPACE),
    insert(
        "http://optimedev.com/", 
        ?TEST_COLUMNFAMILY, 
        "col1_test",
        "test value, test, text, test",
        utils:unix_timestamp(),
        ?cassandra_ConsistencyLevel_ONE
    ),
    
    system_drop_keyspace(?TEST_KEY_SPACE),
    ?assertError(badarith, 6/0),
    ok.

insert6_test()-> 
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    system_add_column_family(?TEST_KEY_SPACE, ?TEST_COLUMNFAMILY),
    set_keyspace(?TEST_KEY_SPACE),
    insert(
        "00001",
        ?TEST_COLUMNFAMILY,
        "col1_test",
        "test value, test, text, test",
        utils:unix_timestamp(),
        ?cassandra_ConsistencyLevel_ONE
    ),
    
    system_drop_keyspace(?TEST_KEY_SPACE),
    ?assertError(badarith, 6/0),
    ok.


get_test()->
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    system_add_column_family(?TEST_KEY_SPACE, ?TEST_COLUMNFAMILY),
    set_keyspace(?TEST_KEY_SPACE),

    insert(
        "00001", 
        ?TEST_COLUMNFAMILY, 
        "col1_test", 
        "test value, test, text, test", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),
     
    try get("00001", ?TEST_COLUMNFAMILY, "col1_test", ?cassandra_ConsistencyLevel_ONE) of 
        {ok, {ok, #columnOrSuperColumn{column = Column} }} ->
            #column{name=Name} = Column,
            ?assertEqual(is_binary(Name), true),
            system_drop_keyspace(?TEST_KEY_SPACE)
    catch   
        { _, {exception, {notFoundException} = Err}} -> 
        system_drop_keyspace(?TEST_KEY_SPACE),
        ?assertEqual(false,true)
    end,
    ok.


get_super_test()->
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    set_keyspace(?TEST_KEY_SPACE),
    {ok, {ok, Addres}} = system_add_column_family(?TEST_KEY_SPACE, ?TEST_COLUMNFAMILY, "Super"),
    ?assert(is_binary(Addres)=:=true),
    insert(
        "00001", 
        ?TEST_COLUMNFAMILY,
        "tsc", 
        "col1_test", 
        "test value, test, text, test", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE 
    ),

    try get("00001", ?TEST_COLUMNFAMILY, "tsc", "col1_test", ?cassandra_ConsistencyLevel_ONE) of
        {ok, {ok, #columnOrSuperColumn{column = Column} }} ->
            #column{name=Name} = Column,
            ?assertEqual(is_binary(Name), true),
            system_drop_keyspace(?TEST_KEY_SPACE)
    catch   
        { _, {exception, {notFoundException} = Err}} -> 
        system_drop_keyspace(?TEST_KEY_SPACE),
        ?assertEqual(false,true)
    end,
    ok.


get_count_test()->
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    set_keyspace(?TEST_KEY_SPACE),    
    system_add_column_family(?TEST_KEY_SPACE, ?TEST_COLUMNFAMILY),

    insert(
        "00002", 
        ?TEST_COLUMNFAMILY, 
        "fname", 
        "Maxim", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),
    insert(
        "00002", 
        ?TEST_COLUMNFAMILY, 
        "lname", 
        "Molchanov", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),
    insert(
        "00002", 
        ?TEST_COLUMNFAMILY, 
        "age", 
        "23", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),

    {ok,{ok, 3}} = get_count(
            "00002", 
            ?TEST_COLUMNFAMILY, undefined, 
            slice("",""),
            ?cassandra_ConsistencyLevel_ONE),

    system_drop_keyspace(?TEST_KEY_SPACE), 
    ok.


get_slice_test()->
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    set_keyspace(?TEST_KEY_SPACE),    
    system_add_column_family(?TEST_KEY_SPACE, ?TEST_COLUMNFAMILY),

    insert(
        "00002", 
        ?TEST_COLUMNFAMILY, 
        "fname", 
        "Maxim", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),
    insert(
        "00002", 
        ?TEST_COLUMNFAMILY, 
        "lname", 
        "Molchanov", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),
    insert(
        "00002", 
        ?TEST_COLUMNFAMILY, 
        "age", 
        "23", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),

    {ok,{ok,Res}} = get_slice(
        "00002",
        #columnParent{column_family=?TEST_COLUMNFAMILY, super_column=undefined},
        #slicePredicate{column_names=["age","lname"], slice_range=undefined},
        ?cassandra_ConsistencyLevel_ONE
    ),

    ?assertEqual(is_list(Res), true),
    ?assertEqual(length(Res), 2),

    system_drop_keyspace(?TEST_KEY_SPACE), 
    ok.


multiget_slice_test()->
    system_add_keyspace(?TEST_KEY_SPACE, "SimpleStrategy"),
    set_keyspace(?TEST_KEY_SPACE),    
    system_add_column_family(?TEST_KEY_SPACE, ?TEST_COLUMNFAMILY),

    insert(
        "00002", 
        ?TEST_COLUMNFAMILY, 
        "fname", 
        "Maxim", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),
    insert(
        "00002", 
        ?TEST_COLUMNFAMILY, 
        "lname", 
        "Molchanov", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),
    insert(
        "00002", 
        ?TEST_COLUMNFAMILY, 
        "age", 
        "23", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),

    insert(
        "00003", 
        ?TEST_COLUMNFAMILY, 
        "fname", 
        "Maxim2", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),
    insert(
        "00003", 
        ?TEST_COLUMNFAMILY, 
        "lname", 
        "Molchanov2", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),
    insert(
        "00003", 
        ?TEST_COLUMNFAMILY, 
        "age", 
        "232", 
        utils:unix_timestamp(), 
        10, 
        ?cassandra_ConsistencyLevel_ONE
    ),

    {ok,{ok,Dict}} = multiget_slice(
        ["00002","00003"],
        #columnParent{column_family=?TEST_COLUMNFAMILY, super_column=undefined},
        #slicePredicate{column_names=["age","lname"], slice_range=undefined},
        ?cassandra_ConsistencyLevel_ONE
    ),

    ?assertEqual(dict:size(Dict),2),

    utils:test_avg(
        optimecass, 
        multiget_slice, 
        [
            ["00002","00003"],
            #columnParent{column_family=?TEST_COLUMNFAMILY, super_column=undefined},
            #slicePredicate{column_names=["age","lname"], slice_range=undefined},
            ?cassandra_ConsistencyLevel_ONE
        ],
        100
    ),

    system_drop_keyspace(?TEST_KEY_SPACE), 
    ok.

-ifdef(NOTGOOD).
-endif.