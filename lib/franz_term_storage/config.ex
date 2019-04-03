defmodule FranzTermStorage.Config do
  def start_worker do
    opts = [uris: KafkaEx.Config.brokers(),
      consumer_group: Application.get_env(:kafka_ex, :consumer_group, "kafka_ex")]

    apply(KafkaEx.Config.server_impl, :start_link, [opts, :no_name])
  end
end
