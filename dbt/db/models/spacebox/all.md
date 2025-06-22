erDiagram
  raw_block_results:::BASE_TABLE
  raw_block_results_consumer:::VIEW
  raw_block_results_topic:::FOREIGN_TABLE
  message_event:::BASE_TABLE
  message_event_block_writer:::VIEW
  message_event_txs_writer:::VIEW
  dex_message_event:::BASE_TABLE
  dex_message_event_writer:::VIEW

  dex_vaults_config_event:::BASE_TABLE
  dex_vaults_config_event_writer:::VIEW
  dex_vaults_config_state:::PROJECTION
  dex_vaults_config_tx_event:::BASE_TABLE
  dex_vaults_config_tx_event_writer:::VIEW

  dex_vaults_dex_balance:::BASE_TABLE
  dex_vaults_dex_balance_by_height:::PROJECTION
  dex_vaults_dex_balance_deposit_writer:::VIEW
  dex_vaults_dex_balance_state:::PROJECTION
  dex_vaults_dex_balance_withdrawal_writer:::VIEW

  dex_vaults_shares:::BASE_TABLE
  dex_vaults_shares_deposit_writer:::VIEW
  dex_vaults_shares_state:::PROJECTION
  dex_vaults_shares_valued:::BASE_TABLE
  dex_vaults_shares_valued_writer:::VIEW
  dex_vaults_shares_valued_again_writer:::REFRESHABLE_VIEW

  raw_block_results_topic ||--|| raw_block_results_consumer : kafka-stream
  raw_block_results_consumer ||--|| raw_block_results : writes-to

  raw_block_results ||--|| message_event_block_writer : incremental
  message_event_block_writer ||--|{ message_event : writes-to
  raw_block_results ||--|| message_event_txs_writer : incremental
  message_event_txs_writer ||--|{ message_event : writes-to

  message_event ||--|| dex_vaults_config_tx_event_writer : incremental
  dex_vaults_config_tx_event_writer ||--o{ dex_vaults_config_tx_event : writes-to

  message_event ||--|| dex_vaults_config_event_writer : incremental
  dex_vaults_config_event_writer ||--o{ dex_vaults_config_event : writes-to
  dex_vaults_config_event }|--|| dex_vaults_config_state : aggregated-to

  message_event ||--|| dex_message_event_writer : incremental
  dex_message_event_writer ||--o{ dex_message_event : writes-to

  dex_message_event ||--|| dex_vaults_shares_deposit_writer : incremental
  dex_vaults_shares_deposit_writer ||--o{ dex_vaults_shares : writes-to
  dex_vaults_shares }|--|| dex_vaults_shares_state : aggregated-to


  dex_message_event ||--|| dex_vaults_dex_balance_withdrawal_writer : incremental
  dex_vaults_dex_balance_withdrawal_writer ||--o{ dex_vaults_dex_balance : writes-to

  dex_message_event ||--|| dex_vaults_dex_balance_deposit_writer : incremental
  dex_vaults_dex_balance_deposit_writer ||--o{ dex_vaults_dex_balance : writes-to
  dex_vaults_dex_balance }|--|| dex_vaults_dex_balance_by_height : aggregated-to
  dex_vaults_dex_balance }|--|| dex_vaults_dex_balance_state : aggregated-to

  dex_vaults_shares ||--|| dex_vaults_shares_valued_writer : incremental
  QueryVaultCurrentValuePoint ||..|| dex_vaults_shares_valued_writer : incremental-join
  dex_vaults_shares_valued_writer ||--o{ dex_vaults_shares_valued : writes-to

  slinky_pairs_state ||..|| QueryDenomCurrentValuePoint : query
  slinky_prices_state ||..|| QueryDenomCurrentValuePoint : query
  slinky_prices_first_state ||..|| QueryDenomCurrentValuePoint : query

  %%dex_vaults_shares ||..|| QueryVaultCurrentValuePoint : query
  dex_vaults_config_state ||..|| QueryVaultCurrentValuePoint : query
  QueryDenomCurrentValuePoint ||..|| QueryVaultCurrentValuePoint : query

  slinky_pairs_state ||..|| QueryDenomValuePoint : query
  slinky_prices ||..|| QueryDenomValuePoint : query
  slinky_prices_first_state ||..|| QueryDenomValuePoint : query

  %%dex_vaults_shares ||..|| QueryVaultValuePoint : query
  dex_vaults_config_state ||..|| QueryVaultValuePoint : query
  QueryDenomValuePoint ||..|| QueryVaultValuePoint : query

  dex_vaults_shares_valued ||..|| dex_vaults_shares_valued_again_writer : refreshable
  QueryVaultValuePoint ||..|| dex_vaults_shares_valued_again_writer : refreshable
  dex_vaults_shares_valued_again_writer ||--o{ dex_vaults_shares_valued : writes-to


  %% PnL %%
  dex_vaults_shares_valued ||..|{ PNL : timeseries
  dex_vaults_dex_balance ||..|{ PNL : timeseries
  QueryDenomValuePoint ||..|{ PNL : timeseries

%%CREATE MATERIALIZED VIEW spacebox.message_event_block_writer TO spacebox.message_event%%
%%message_event_block_writer ||--|{ message_event : writes-to%%

  %% Slinky %%
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
  classDef VIEW stroke:#0f0,rx:16
  classDef REFRESHABLE_VIEW stroke:#0f0,rx:16,stroke-dasharray:10
  classDef PROJECTION stroke:#0ff