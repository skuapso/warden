-module(warden).

-behaviour(application).
-behaviour(supervisor).

%% hooks
-export([
  terminal_packet/7
  ]).

%% API
-export([
  start/0,
  start/2,
  start_link/0,
  start_link/1,
  stop/1,
  status/1
  ]).

%% Supervisor callbacks
-export([init/1]).

-include_lib("logger/include/log.hrl").
%%%===================================================================
%%% API functions
%%%===================================================================
status(Query) ->
  warden_pool:status(self(), Query).

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
terminal_packet(_Pid, _Module, _UIN, Type, _RawData, _Packet, _Timeout)
    when ((Type =:= authentication) or (Type =:= unknown) or (Type =:= broken))
    ->
  ok;
terminal_packet(_Pid, _Module, UIN, Type, _RawData, Packet, _Timeout) ->
  trace("processing terminal packet"),
  {ok, Pid} = warden_pool:get_first(ready),
  warden_worker:execute(Pid, prepare_packet(UIN, Type, Packet)),
  ok.

start() ->
  application:start(?MODULE).

start(_StartType, StartArgs) ->
  trace("starting application"),
  ?MODULE:start_link(StartArgs).

stop(_State) ->
  ok.

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_link(StartArgs) ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, StartArgs).
%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init(Opts) ->
  MaxConnections = misc:get_env(?MODULE, max_connections, Opts),
  Weight = misc:get_env(?MODULE, weight, Opts),
  hooks:install(terminal_packet, Weight, fun ?MODULE:terminal_packet/7),
  {ok, 
    {
      {one_for_one, 5, 10},
      [
        {
          warden_pool,
          {warden_pool, start_link, [get_opts(Opts), MaxConnections]},
          permanent,
          5000,
          worker,
          []
        }
      ]
    }
  }.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_opts(Opts) ->
  Host    = misc:get_env(?MODULE, host, Opts),
  Port    = misc:get_env(?MODULE, port, Opts),
  User    = misc:get_env(?MODULE, user, Opts),
  Passwd  = misc:get_env(?MODULE, password, Opts),
  DB      = misc:get_env(?MODULE, database, Opts),
  SSL     = misc:get_env(?MODULE, ssl, Opts),
  SSLOpts = misc:get_env(?MODULE, ssl_opts, Opts),
  Timeout = misc:get_env(?MODULE, timeout, Opts),
  [Host, Port, User, Passwd, DB, SSL, SSLOpts, Timeout].

prepare_packet(UIN, Type, Packet) ->
  trace("prepare packet"),
  N = proplists:get_value(navigation, Packet, []),
  S = proplists:get_value(set, Packet, []),
  DI = proplists:get_value(digital_in, S, []),
  DO = proplists:get_value(digital_out, S, []),
  A = proplists:get_value(analog, S, []),
  C = proplists:get_value(counter, S, []),
  {[
    UIN,
    proplists:get_value(eventtime, N, proplists:get_value(terminal_eventtime, N, {{1970, 1, 1}, {0, 0, 0}})),
    convert_msg_type(Type),
    proplists:get_value(action_no, N, 0),
    proplists:get_value(terminal_data_id, N, 0),
    ll:float(proplists:get_value(latitude, N, 0)),
    ll:float(proplists:get_value(longitude, N, 0)),
    proplists:get_value(speed, N, 0),
    proplists:get_value(course, N, 0),
    proplists:get_value(altitude, N, 0),
    proplists:get_value(visible, N, 24),
    proplists:get_value(used, N, 0),
    proplists:get_value(gps_state, N, 2),
    proplists:get_value(terminal_state, N, 0),
    proplists:get_value(signal, N, proplists:get_value(gsm, proplists:get_value(signal, S, []), 0)),
    misc:convert_digital(DI),
    misc:convert_digital(DO),
    proplists:get_value(internal_power, N, 0),
    proplists:get_value(external_power, N, 0)
      ], convert_analog_counter(A, C)}.

convert_analog_counter(Analog, Counter) ->
  [
    {1, proplists:get_value(1, Analog, 0)},
    {2, proplists:get_value(2, Analog, 0)},
    {3, proplists:get_value(3, Analog, 0)},
    {4, proplists:get_value(4, Analog, 0)},
    {5, proplists:get_value(1, Counter, 0)},
    {6, proplists:get_value(2, Counter, 0)}
  ].

convert_msg_type(online) ->
  1;
convert_msg_type(offline) ->
  0;
convert_msg_type(_) ->
  3.
