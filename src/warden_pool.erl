-module(warden_pool).

-behaviour(gen_server).

%% API
-export([
  start_link/2,
  status/2,
  get_first/1
  ]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-record(state, {opts, max_connections = 0}).

-include_lib("logger/include/log.hrl").
%%%===================================================================
%%% API
%%%===================================================================
status(Pid, Query) ->
  ets:insert(?MODULE, {Pid, Query}).

get_first(Status) ->
  case ets:match(?MODULE, {'$1', Status}, 1) of
    {[[Pid]], _} ->
      {ok, Pid};
    '$end_of_table' ->
      new()
  end.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Opts, MaxConnections) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {Opts, MaxConnections}, []).

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
init({Opts, MaxConnections}) ->
  trace("init"),
  process_flag(trap_exit, true),
  ets:new(?MODULE, [set, public, named_table]),
  {ok, #state{opts = Opts, max_connections = MaxConnections}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(new, _From, #state{opts = Opts, max_connections = M} = S) ->
  MatchSpec = [{'_', [], [true]}],
  C = ets:select_count(?MODULE, MatchSpec),
  Reply = new(Opts, C, M),
  {reply, Reply, S};
handle_call(Request, From, State) ->
  warning("unhandled call ~w from ~w", [Request, From]),
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
  warning("unhandled cast ~w", [Msg]),
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
handle_info({'EXIT', Pid, _}, State) ->
  ets:delete(?MODULE, Pid),
  {noreply, State};
handle_info(Info, State) ->
  warning("unhandled info ~w", [Info]),
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
new() ->
  gen_server:call(?MODULE, new).

new(Opts, C, M)
    when (M =:= infinity)
    or (M < 0)
    or (M =:= 0)
    or (C < M)
    ->
  warden_worker:start_link(Opts);
new(_, _, _) ->
  {error, max_connections_reached}.
