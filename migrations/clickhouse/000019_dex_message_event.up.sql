
CREATE TABLE spacebox.dex_message_event
(
    `timestamp` DateTime,
    `height` Int64,
    `block_part_index` Int8,
    `tx_index` Int32,
    -- msg data
    `msg_part_index` Int16,
    `msg_indexes` Array(Int16),
    -- wasm msg data
    `wasm_part_index` Int16,
    -- event data
    `msg_part_events_index_offset` Int32,
    `msg_part_events` Array(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (
    height,
    block_part_index,
    tx_index,
    msg_part_index,
    wasm_part_index
)
SETTINGS index_granularity = 8192;

-- spacebox.dex_message_event_writer source

CREATE MATERIALIZED VIEW spacebox.dex_message_event_writer TO spacebox.dex_message_event
(
    `timestamp` DateTime,
    `height` Int64,
    `block_part_index` Int8,
    `tx_index` Int32,
    -- msg data
    `msg_part_index` Int16,
    `msg_indexes` Array(Int16),
    -- wasm msg data
    `wasm_part_index` Int16,
    -- event data
    `msg_part_events_index_offset` Int32,
    `msg_part_events` Array(String)
) AS
WITH
    -- define join tuple parts for row fields
    tx_message_tuple.1 as `wasm_part_index`,
    tx_message_tuple.2 as `msg_part_events_index_offset`,
    tx_message_tuple.3 as `msg_part_events`
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `msg_part_index`,
    `msg_indexes`,
    `wasm_part_index`,
    `msg_part_events_index_offset`,
    `msg_part_events`
FROM
    spacebox.message_event
    ARRAY JOIN (
        -- Extract "DEX wasm event fingerprint" from events to extract wasm msg events
        arrayFlatten(
            arrayMap(
                (msg_events__types) -> (
                    -- test if WASM msg part decomposition is required
                    if (
                        -- todo: replace check with height condition when fix release version height is known
                        --       eg. `height < 25000000`
                        -- current check: decompose all wasm msgs that don't have TickUpdate.SwapAmountIn attributes
                        not(has(msg_events__types, 'wasm')) OR
                        arrayExists(
                            -- test for SwapAmountIn presence on TickUpdate events
                            (msg_event) -> (
                                JSONExtractString(msg_event, 'type') = 'TickUpdate' AND
                                arrayExists(
                                    (attr) -> JSONExtractString(attr, 'key') = 'SwapAmountIn',
                                    JSONExtractArrayRaw(msg_event, 'attributes')
                                )
                            ),
                            msg_events
                        ),
                        -- pass the message as not requiring decomposition
                        [[(
                            -- tx_message_tuple.1: wasm_part_index
                            toInt16(0),
                            -- tx_message_tuple.2: msg_part_events_index_offset
                            `msg_events_index_offset`,
                            -- tx_message_tuple.2: msg_part_events
                            `msg_events`
                        )]],
                        -- compute out all the DEX msg parts of each message
                        arrayMap(
                            (msg_events__wasm_dex_msg_event_indexes) -> arrayMap(
                                (wasm_dex_msg_event_index_lower_bound, wasm_dex_msg_event_index_upper_bound, wasm_part_index) -> (
                                    -- tx_message_tuple.1: wasm_part_index
                                    toInt16(wasm_part_index - 1),
                                    -- tx_message_tuple.2: msg_part_events_index_offset
                                    toInt32(`msg_events_index_offset` + wasm_dex_msg_event_index_lower_bound - 1),
                                    -- tx_message_tuple.2: msg_part_events
                                    arraySlice(
                                        `msg_events`,
                                        wasm_dex_msg_event_index_lower_bound,
                                        wasm_dex_msg_event_index_upper_bound - wasm_dex_msg_event_index_lower_bound
                                    )
                                ),
                                -- pass start of bounds
                                arrayConcat([1], msg_events__wasm_dex_msg_event_indexes),
                                -- pass end of bounds
                                arrayConcat(msg_events__wasm_dex_msg_event_indexes, [length(msg_events__types) + 1]),
                                -- wasm_part_index
                                arrayEnumerate(arrayConcat([1], msg_events__wasm_dex_msg_event_indexes))
                            ),
                            arrayMap(
                                -- precompute msg_events "fields" as arrayMap lambda arguments
                                -- - msg_events "field" msg_events__wasm_dex_msg_event_indexes
                                (msg_events__wasm_dex_msg_regex_matches) -> arraySort(
                                    -- protect against found indexes of "0", those are not matches
                                    -- also remove any "1" matches because we will add a "1" lower bound index later
                                    arrayFilter(
                                        (i) -> i > 1,
                                        arrayFlatten(
                                            -- find msg event bounds within wasm actions
                                            arrayMap(
                                                (match, match_count_index) -> (
                                                    arrayFilter(
                                                        i -> arraySlice(msg_events__types, i, length(splitByChar(',', match))) = splitByChar(',', match),
                                                        arrayEnumerate(msg_events__types)
                                                    )[match_count_index]
                                                ),
                                                msg_events__wasm_dex_msg_regex_matches,
                                                -- find the "match_count_index" number of the each match string by counting the number of previously seen matching match strings
                                                -- (eg. if a PlaceLimitOrder match was detected, is it PlaceLimitOrder 1 or 2 or N?)
                                                arrayMap(
                                                    (match, i) -> arrayCount(x -> x = match, arraySlice(msg_events__wasm_dex_msg_regex_matches, 1, i - 1)) + 1,
                                                    msg_events__wasm_dex_msg_regex_matches,
                                                    arrayEnumerate(msg_events__wasm_dex_msg_regex_matches)
                                                )
                                            )
                                        )
                                    )
                                ),
                                -- precompute msg_events "fields" as arrayMap lambda arguments
                                -- - msg_events "field" msg_events__wasm_dex_msg_regex_matches
                                [arrayFilter(
                                    x -> notEmpty(x),
                                    arrayFlatten(
                                        -- compare tx event types array as string against tx msg detection regex
                                        -- to find where the sub-msgs are in each CosmWasm tx `events` list
                                        extractAllGroupsHorizontal(
                                            arrayStringConcat(msg_events__types, ','),
                                            -- note: this is a msg action detection regex, it can determine which Dex v5 msg was used to create this order of events
                                            --       msgs: https://github.com/neutron-org/neutron/blob/v5.1.3/proto/neutron/dex/tx.proto#L16-L28
                                            arrayStringConcat(
                                                [
                                                    '(',
                                                    arrayStringConcat([
                                                        -- MsgDeposit
                                                        '(?:message,)?(?:(?:neutron,)?(?:neutron,)?(?:TickUpdate,)?TickUpdate,)+(?:message,)*(?:coin_spent,coin_received,transfer,(?:message,)?)?coin_spent,coin_received,transfer,(?:message,)?coin_received,coinbase,coin_spent,coin_received,transfer(?:,message)?',
                                                        -- MsgWithdrawal
                                                        '(?:message,)?(?:(?:neutron,)?TickUpdate,)+(?:message,)*coin_spent,coin_received,transfer,(?:message,)?coin_spent,burn,coin_spent,coin_received,transfer(?:,message)?,neutron',
                                                        -- MsgPlaceLimitOrder
                                                        '(?:message,)?(?:(?:neutron,)?TickUpdate(?:,TickUpdate)?,)*neutron,(?:neutron,)?(?:TickUpdate,)?TrancheUserUpdate,(?:coin_spent,coin_received,transfer,(?:message,)?)?coin_spent,coin_received,transfer(?:,message)?',
                                                        -- MsgWithdrawFilledLimitOrder
                                                        -- (unused) '(?:message,)?TrancheUserUpdate,coin_spent,coin_received,transfer(?:,message)?(?:,message)?',
                                                        -- MsgCancelLimitOrder
                                                        '(?:message,)?(?:TrancheUserUpdate,(?:neutron,)?TickUpdate,)+(?:coin_spent,coin_received,transfer,(?:message,)?)?coin_spent,coin_received,transfer(?:,message)?(?:,message)?',
                                                        -- MsgMultiHopSwap
                                                        '(?:message,)?(?:(?:neutron,)?TickUpdate(?:,TickUpdate)?,)*neutron,(?:TickUpdate,)?coin_spent,coin_received,transfer,(?:message,)?coin_spent,coin_received,transfer(?:,message)?'
                                                    ], ')|('),
                                                    ')'
                                                ],
                                                ''
                                            )
                                        )
                                    )
                                )]
                            )
                        )
                    )
                ),
                -- precompute msg_events "fields" as arrayMap lambda arguments
                -- - msg_events "field" msg_events__types
                arrayFilter(
                    -- filter to only DEX messages by testing for "TickUpdate" or "TrancheUserUpdate" actions
                    -- note: this may change in the future, but if no update has happened, its not really a DEX action
                    (msg_events__types) -> (
                        has(msg_events__types, 'TickUpdate') OR
                        has(msg_events__types, 'TrancheUserUpdate')
                    ),
                    [arrayMap(
                        (msg_event) -> JSONExtractString(msg_event, 'type'),
                        msg_events
                    )]
                )
            )
        )
    ) AS `tx_message_tuple`
SETTINGS
    -- split query execution into small chunks to reduce peak memory usage
    max_block_size = 50;
