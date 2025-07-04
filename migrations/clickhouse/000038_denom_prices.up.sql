
-- spacebox.denoms_source_data_from_skip definition

CREATE DICTIONARY spacebox.denoms_source_data_from_skip (
    `denom`                 String,
    `origin_chain_id`       String,
    `origin_denom`          String,
    `name`                  String,
    `symbol`                String,
    `recommended_symbol`    String,
    `decimals`              UInt8
)
PRIMARY KEY `denom`
SOURCE(FILE(path './user_files/mainnet/skip-assets.json' format 'JSONEachRow'))
LAYOUT(HASHED)
LIFETIME(3600);
