# FranzTermStorage

FranzTermStorage is a behaviour module that can cache all kafka data in named public ets.

## Using FranzTermStorage

Let's start with code example.

FranzTermStorage depens on kafka_ex library. You should specify kafka address in config:
```elixir
config :kafka_ex,
  brokers: [{"localhost", 9092}]
```

Kafka_ex fails to start when kafka is not working. To disable this behaviour add
```elixir
config :kafka_ex,
  disable_default_worker: true
```

All callbacks are optional. So you can write just this:
```elixir
iex(1)> defmodule Test do
...(1)>   use FranzTermStorage
...(1)> end

iex(2)> FranzTermStorage.start_link(Test, "topic_name", reconnect_interval: 1000)
{:ok, #PID<0.181.0>}

iex(3)> :ets.tab2list(Test)
[]
```

Ets `Test` will be populated with all data in kafka topic "topic_name". You can add data with
```
sh-4.4$ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic topic_name \
  --property "parse.key=true"  --property "key.separator=:"
>key:value
```

```elixir
iex(4)> :ets.tab2list(Test)
[{"key", "value"}]
```

You can preprocess kafka messages before they will be inserted to ets. For example:

```elixir
def handle_messages(list) do
  list
  |> Enum.map(fn {k, v} ->
      case Integer.parse(v) do
        :error -> nil
        {n, _} -> {k, n}
      end
  end)
  |> Enum.reject(&is_nil/1)
end
```

You can implement handle_sync to be notified when all current data has been read

```elixir
def handle_sync() do
  Logger.info("topic_name topic has been read")
  Registry.register(Registry.UniqueRegisterTest, "topic_name", Test)
end
```

FranzTermStorage will safety work with kafka downtime. Just retry to connect every second (by default)
