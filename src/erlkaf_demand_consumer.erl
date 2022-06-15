-module(erlkaf_demand_consumer).

-include("erlkaf_private.hrl").

-define(DEFAULT_BATCH_SIZE, 1000).

-behaviour(gen_server).

-export([
    start_link/6,
    stop/1,
    poll_events/0,

    % genserver

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    client_ref,
    topic_name,
    partition,
    queue_ref,
    cb_module,
    cb_state,
    poll_batch_size,
    poll_idle_ms,
    messages = [],
    last_offset = -1
}).


start_link(ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings) ->
    gen_server:start_link(?MODULE, [ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings], []).

stop(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Tag = make_ref(),
            Pid ! {stop, self(), Tag},

            receive
                {stopped, Tag} ->
                    ok
            after 5000 ->
                exit(Pid, kill)
            end;
        _ ->
            {error, not_alive}
    end.

init([ClientRef, TopicName, Partition, Offset, QueueRef, TopicSettings]) ->
    ?LOG_INFO("start demand consumer for: ~p partition: ~p offset: ~p", [TopicName, Partition, Offset]),

    CbModule = erlkaf_utils:lookup(callback_module, TopicSettings),
    CbArgs = erlkaf_utils:lookup(callback_args, TopicSettings, []),
    PollBatchSize = erlkaf_utils:lookup(poll_batch_size, TopicSettings, ?DEFAULT_BATCH_SIZE),

    case catch CbModule:init(TopicName, Partition, Offset, CbArgs) of
        {ok, CbState} ->
            
            {ok, #state{
                client_ref = ClientRef,
                topic_name = TopicName,
                partition = Partition,
                queue_ref = QueueRef,
                cb_module = CbModule,
                cb_state = CbState,
                poll_batch_size = PollBatchSize
            }};
        Error ->
            ?LOG_ERROR("~p:init for topic: ~p failed with: ~p", [CbModule, TopicName, Error]),
            {stop, Error}
    end.

poll_events() ->
    erlkaf_utils:safe_call(?MODULE, poll_events).

handle_call(poll_events, _From, #state{queue_ref = Queue, poll_batch_size = PollBatchSize, client_ref = ClientRef, topic_name = Topic, partition = Partition} = State) ->    
    case erlkaf_nif:consumer_queue_poll(Queue, PollBatchSize) of
        {ok, Events, LastOffset} ->
            case Events of
                [] ->
                   [];
                _ ->
                    ok = erlkaf_nif:consumer_offset_store(ClientRef, Topic, Partition, LastOffset)
            end,
            {reply, Events, State};
        Error ->
            ?LOG_INFO("~p poll events error: ~p", [?MODULE, Error]),
            throw({error, Error})
    end.

handle_cast(Request, State) ->
    ?LOG_ERROR("handle_cast unexpected message: ~p", [Request]),
    {noreply, State}.

handle_info({stop, From, Tag}, State) ->
    handle_stop(From, Tag, State),
    {stop, normal, State};

handle_info(Info, State) ->
    ?LOG_ERROR("handle_info unexpected message: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_stop(From, Tag, #state{topic_name = TopicName, partition = Partition, queue_ref = Queue}) ->
    ?LOG_INFO("stop consumer for: ~p partition: ~p", [TopicName, Partition]),
    ok = erlkaf_nif:consumer_queue_cleanup(Queue),
    From ! {stopped, Tag}.