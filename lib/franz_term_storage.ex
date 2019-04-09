defmodule FranzTermStorage do
  use GenServer

  require Logger

  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse

  @callback handle_messages(List.t()) :: :ok
  @callback handle_sync() :: :ok

  @optional_callbacks handle_sync: 0, handle_messages: 1

  @enforce_keys [:topic, :name, :interval]
  defstruct [:topic, :ets, :pid, :name, :interval]

  defmacro __using__(_opts) do
    quote do
      @behaviour FranzTermStorage

      def handle_sync do
        :ok
      end

      def handle_messages(list) do
        list
      end

      defoverridable handle_sync: 0, handle_messages: 1
    end
  end

  def start_link(name, topic, opts \\ []) do
    state = %__MODULE__{
      topic: topic,
      name: name,
      interval: Keyword.get(opts, :reconnect_interval, 1000)
    }

    GenServer.start_link(__MODULE__, state)
  end

  @impl true
  def init(state) do
    Process.flag(:trap_exit, true)
    ets = :ets.new(state.name, [:named_table, :public])
    Process.send_after(self(), :start, 0)
    {:ok, %{state | ets: ets}}
  end

  @impl true
  def handle_info(:start, %{topic: topic} = state) do
    mod = state.name

    {:ok, pid} =
      Task.start_link(fn ->
        {:ok, worker} = start_worker()

        [
          %OffsetResponse{
            partition_offsets: [%{error_code: :no_error, offset: [earliest_offset]}]
          }
        ] = KafkaEx.earliest_offset(topic, 0, worker)

        [%OffsetResponse{partition_offsets: [%{error_code: :no_error, offset: [start_offset]}]}] =
          KafkaEx.latest_offset(topic, 0, worker)

        KafkaEx.stream(state.topic, 0, offset: earliest_offset, worker_name: worker)
        |> Stream.map(fn e ->
          objects = mod.handle_messages([{e.key, e.value}])
          :ets.insert(state.ets, objects)

          if e.offset + 1 == start_offset do
            mod.handle_sync()
          end
        end)
        |> Stream.run()
      end)

    {:noreply, %{state | pid: pid}}
  end

  def handle_info({:EXIT, pid, reason}, %{pid: pid} = state) do
    Logger.error("consumer failed with #{inspect(reason)}. Retry after #{state.interval}")
    Process.send_after(self(), :start, state.interval)
    {:noreply, state}
  end

  def handle_info({:EXIT, pid, reason}, state) do
    Logger.error("exit #{inspect(pid)} with #{inspect(reason)}")
    {:noreply, state}
  end

  def start_worker do
    opts = [
      uris: KafkaEx.Config.brokers(),
      consumer_group: Application.get_env(:kafka_ex, :consumer_group, "kafka_ex")
    ]

    apply(KafkaEx.Config.server_impl(), :start_link, [opts, :no_name])
  end
end
