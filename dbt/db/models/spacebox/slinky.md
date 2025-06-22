erDiagram
  raw_slinky_prices:::BASE_TABLE
  raw_slinky_prices_topic:::FOREIGN_TABLE
  slinky_pairs:::BASE_TABLE
  slinky_pairs_state:::PROJECTION
  slinky_pairs_writer:::VIEW
  slinky_prices:::BASE_TABLE
  slinky_prices_first_state:::PROJECTION
  slinky_prices_state:::PROJECTION
  slinky_prices_writer:::VIEW

  raw_slinky_prices_topic }|--|| raw_slinky_prices : kafka-stream

  raw_slinky_prices ||--|| slinky_pairs_writer : incremental
  slinky_pairs_writer ||--|{ slinky_pairs : writes-to
  slinky_pairs }|--|| slinky_pairs_state : aggregated-to

  raw_slinky_prices ||--|| slinky_prices_writer : incremental
  slinky_prices_writer ||--|{ slinky_prices : writes-to
  slinky_prices }|--|| slinky_prices_state : aggregated-to
  slinky_prices }|--|| slinky_prices_first_state : aggregated-to

  classDef FOREIGN_TABLE stroke:#00f
  classDef BASE_TABLE stroke:#f00
  classDef VIEW stroke:#0f0
  classDef PROJECTION stroke:#0ff