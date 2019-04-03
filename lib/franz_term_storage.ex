defmodule FranzTermStorage do
  use GenServer

  require Logger

  alias FranzTermStorage.Consumer

  @callback handle_messages(List.t) :: :ok
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

      defoverridable  handle_sync: 0, handle_messages: 1
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
  def handle_info(:start, state) do
    mod = state.name
    case Consumer.start_link(state.topic, state.ets, mod) do
      {:ok, pid} ->
        Logger.debug("FranzTermStorage.Consumer started")
        {:noreply, %{state | pid: pid}}

      {:error, reason} ->
        Logger.error("FranzTermStorage.Consumer start error #{inspect(reason)}. Retry after #{state.interval}")
        Process.send_after(self(), :start, state.interval)
        {:noreply, state}
    end
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
end
