-module(warden_worker).

-behaviour(gen_fsm).

%% API
-export([
  start_link/1,
  start_link/8,
  status/1,
  execute/2
  ]).

%% gen_fsm callbacks
-export([
  disconnected/2,
  ready/2,
  ready/3
  ]).
-export([
  init/1,
  handle_event/3,
  handle_sync_event/4,
  handle_info/3,
  terminate/3,
  code_change/4
  ]).

-define(WAIT_TIMEOUT, 30000).

-record(state, {
    socket,
    timeout
    }).

-include_lib("logger/include/log.hrl").

%%%===================================================================
%%% API
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
execute(Pid, Data) ->
  gen_fsm:sync_send_event(Pid, Data).

status(Status) ->
  warden:status(Status).

start_link([Host, Port, User, Passwd, DB, SSL, SSLOpts, Timeout]) ->
  start_link(Host, Port, User, Passwd, DB, SSL, SSLOpts, Timeout).

start_link(Host, Port, User, Passwd, DB, SSL, SSLOpts, undefined) ->
  start_link(Host, Port, User, Passwd, DB, SSL, SSLOpts, ?WAIT_TIMEOUT);
start_link(Host, Port, User, Passwd, DB, SSL, SSLOpts, Timeout) ->
  trace("starting"),
  gen_fsm:start_link(?MODULE, [Host, Port, User, Passwd, DB, SSL, SSLOpts, Timeout], [{timeout, Timeout}]).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([Host, Port, User, Passwd, DB, SSL, SSLOpts, Timeout]) ->
  trace("initialization"),
  gen_fsm:send_event(self(), {connect, Host, Port, User, Passwd, DB, SSL, SSLOpts, Timeout}),
  status(connecting),
  {ok, disconnected, #state{}, Timeout}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
disconnected({connect, Host, Port, User, Passwd, DB, SSL, SSLOpts, Timeout}, _S) ->
  trace("connecting"),
  Opts = [{port, Port}, {database, DB}, {ssl, SSL}, {ssl_opts, SSLOpts}, {timeout, infinity}],
  debug("connecting to ~s:~w, user ~s, opts ~w", [Host, Port, User, Opts]),
  {ok, C} = pgsql:connect(Host, User, Passwd, Opts),
  Commands = misc:get_env(warden, pre_commands, []),
  lists:map(fun(X) ->
        debug("running command ~s", [X]),
        A = pgsql:squery(C, X),
        debug("answer is ~w", [A])
    end, Commands),
  trace("connected"),
  status(ready),
  {next_state, ready, #state{socket=C, timeout=Timeout}, Timeout}.

ready(timeout, S) ->
  {stop, normal, S}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
%%  1 - module_uin
%%  2 - eventtime
%%  3 - msg_type
%%  4 - action number
%%  5 - msg number
%%  6 - latitude
%%  7 - longitude
%%  8 - speed
%%  9 - course
%% 10 - altitude
%% 11 - number of visible satellites
%% 12 - number of used satellites
%% 13 - gps state
%% 14 - terminal state
%% 15 - gsm signal
%% 16 - digital sensors
%% 17 - digital out
%% 18 - internal battery volts
%% 19 - external power volts
ready({[UIN | _], _} = {Values, Sensors}, _From, #state{socket = C} = S) ->
  trace("quering"),
  warden:status(quering),
  PreQuery = misc:get_env(warden, pre_query, []),
  Reply = case check_pre_query(C, PreQuery, [UIN]) of
    true ->
      Query = "SELECT ctrl.addData($1::bigint, $2::timestamp without time zone, $3::integer, $4::integer, $5::integer, $6::float, $7::float, $8::float, $9::float, $10::float, $11::integer, $12::integer, $13::integer, $14::integer, $15::float, $16::bigint, $17::bigint, $18::float, $19::float) as id",
      debug("requesting ~s with values ~w", [Query, Values]),
      {ok, _Cols, [{Eid}]} = pgsql:equery(C, Query, Values),
      debug("event id is ~w", [Eid]),
      insert_sensors(C, Eid, Sensors),
      {ok, Eid};
    false ->
      debug("pre check failed for ~w", [UIN]),
      {ok, -1}
  end,
  warden:status(ready),
  debug("query finished"),
  {reply, Reply, ready, S, S#state.timeout}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(normal, ready, #state{socket = C} = _S) ->
  trace("terminating"),
  pgsql:close(C),
  ok;
terminate(Reason, StateName, #state{socket = C} = _S) ->
  warning("terminating when ~w, reason ~w", [StateName, Reason]),
  pgsql:close(C),
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
insert_sensors(_Socket, -1, _Data) ->
  ok;
insert_sensors(_Socket, _Eid, []) ->
  ok;
insert_sensors(Socket, Eid, [{N, V} | T]) ->
  insert_sensor(Socket, Eid, N, V),
  insert_sensors(Socket, Eid, T).

insert_sensor(Socket, Eid, N, V) ->
  pgsql:equery(Socket, "select ctrl.addAnalog($1, $2, $3)", [Eid, N, V]).

check_pre_query(_, [], _) ->
  true;
check_pre_query(C, [Q | T], Val) ->
  case pgsql:equery(C, Q, Val) of
    {ok, [{column, _, bool, 1, -1, 1}], [{true}]} ->
      check_pre_query(C, T, Val);
    Else ->
      debug("cheking query ~w failed with params ~w answer was ~w", [Q, Val, Else]),
      false
  end.
