erDiagram
  raw_block_results:::BASE_TABLE
  raw_block_results_consumer:::VIEW
  raw_block_results_topic:::FOREIGN_TABLE

  raw_block_results_topic ||--|| raw_block_results_consumer : kafka-stream
  raw_block_results_consumer ||--|| raw_block_results : writes-to

  raw_block_results ||--|| message_event_block_writer : incremental
  message_event_block_writer ||--|{ message_event : writes-to
  slinky_pairs }|--|| slinky_pairs_state : aggregated-to

  %%CREATE MATERIALIZED VIEW spacebox.message_event_block_writer TO spacebox.message_event%%
  %%message_event_block_writer ||--|{ message_event : writes-to%%

  classDef FOREIGN_TABLE stroke:#00f
  classDef BASE_TABLE stroke:#f00
  classDef VIEW stroke:#0f0
  classDef PROJECTION stroke:#0ff