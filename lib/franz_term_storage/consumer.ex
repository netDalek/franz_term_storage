defmodule FranzTermStorage.Consumer do
  use GenServer

  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse
  alias KafkaEx.Protocol.Fetch.Response, as: FetchResponse
  alias KafkaEx.Protocol.Fetch.Message

  alias FranzTermStorage.Config

  def start_link(topic, ets, fun) do
    GenServer.start_link(__MODULE__, [topic, ets, fun])
  end

  @impl true
  def init([topic, ets, fun]) do
    {:ok, worker} = Config.start_worker

    [%OffsetResponse{partition_offsets: [%{error_code: :no_error, offset: [offset]}]}] =
      KafkaEx.earliest_offset(topic, 0, worker)

    {:ok, %{worker: worker, topic: topic, offset: offset, ets: ets, fun: fun}, 0}
  end

  @impl true
  def handle_info(:timeout, %{fun: fun, worker: worker, offset: offset} = state) do
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
                  |> fun.()

        :ets.insert(state.ets, objects)

        %Message{offset: last_offset} = List.last(message_set)
        {:noreply, %{state | offset: last_offset+1}, 0}
    end
  end
end
