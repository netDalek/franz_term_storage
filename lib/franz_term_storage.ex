defmodule FranzTermStorage do
  use GenServer

  require Logger

  alias FranzTermStorage.Consumer

  @enforce_keys [:topic, :name, :fun, :interval]
  defstruct [:topic, :ets, :pid, :fun, :name, :interval]

  def start_link(name, topic, parse_fun, opts \\ []) do
    state = %__MODULE__{
      topic: topic,
      name: name,
      fun: parse_fun,
      interval: Keyword.get(opts, :restart_interval, 1000)
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
    case Consumer.start_link(state.topic, state.ets, state.fun) do
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
