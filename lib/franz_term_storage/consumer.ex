defmodule FranzTermStorage.Consumer do
  use GenServer

  require Logger

  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse
  alias KafkaEx.Protocol.Fetch.Response, as: FetchResponse
  alias KafkaEx.Protocol.Fetch.Message

  alias FranzTermStorage.Config

  def start_link(topic, ets, name) do
    GenServer.start_link(__MODULE__, [topic, ets, name])
  end

  @impl true
  def init([topic, ets, name]) do
    {:ok, worker} = Config.start_worker

    [%OffsetResponse{partition_offsets: [%{error_code: :no_error, offset: [earliest_offset]}]}] =
      KafkaEx.earliest_offset(topic, 0, worker)

    [%OffsetResponse{partition_offsets: [%{error_code: :no_error, offset: [start_offset]}]}] =
      KafkaEx.latest_offset(topic, 0, worker)

    {:ok, %{worker: worker, topic: topic, offset: earliest_offset, start_offset: start_offset, ets: ets, name: name}, 0}
  end

  @impl true
  def handle_info(:timeout, %{name: name, worker: worker, offset: offset} = state) do
    [%FetchResponse{partitions: [response = %{error_code: :no_error}]}] =
      KafkaEx.fetch(state.topic, 0,
        offset: offset,
        auto_commit: false,
        worker_name: worker,
        wait_time: 100
      )

    case response.message_set do
      [] ->
        {:noreply, state, 0}

      message_set ->
        objects = message_set
                  |> Enum.map(fn(m) -> {m.key, m.value} end)
                  |> name.handle_messages()

        :ets.insert(state.ets, objects)

        %Message{offset: last_offset} = List.last(message_set)
        case last_offset + 1 >= state.start_offset do
          true ->
            name.handle_sync()
            {:noreply, %{state | offset: last_offset+1, start_offset: :infinity}, 0}

          false ->
            {:noreply, %{state | offset: last_offset+1}, 0}
        end

    end
  end
end
