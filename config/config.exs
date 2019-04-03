use Mix.Config

config :kafka_ex,
  brokers: [
    {"localhost", 9092}
  ],
  sync_timeout: 5000,
  disable_default_worker: true
