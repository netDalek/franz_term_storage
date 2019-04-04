defmodule FranzTermStorageTest do
  use ExUnit.Case
  doctest FranzTermStorage

  require Logger

  setup do
    Application.put_env(:franz_term_storage, :test_process, self())

    {:ok, pid} = FranzTermStorage.start_worker()
    [kafka: pid, topic: random_string(10)]
  end

  defmodule Test do
    use FranzTermStorage

    def handle_sync do
      Application.fetch_env!(:franz_term_storage, :test_process) |> send(:sync)
    end

    def handle_messages(list) do
      Application.fetch_env!(:franz_term_storage, :test_process) |> send(:message)
      list
    end
  end

  test "greets the world", %{kafka: kafka, topic: topic} do
    KafkaEx.produce(topic, 0, "777", worker_name: kafka, key: "key")
    FranzTermStorage.start_link(Test, topic)

    assert_receive :message, 1000
    assert_receive :sync, 1000
    assert [{"key", "777"}] == :ets.tab2list(Test)

    {time, _} = :timer.tc(fn ->
      KafkaEx.produce(topic, 0, "999", worker_name: kafka, key: "key")
      assert_receive :message, 1000
    end)

    Logger.info("kafka latency is #{time} microseconds")
    assert [{"key", "999"}] == :ets.tab2list(Test)
  end

  def random_string(length) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
  end
end
