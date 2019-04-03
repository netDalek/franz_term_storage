use Mix.Config

config :kafka_ex,
  brokers: [
    {"localhost", 9092}
  ],
  disable_default_worker: true
