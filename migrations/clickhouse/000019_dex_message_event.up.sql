
CREATE TABLE spacebox.dex_message_event
(
    `timestamp`                     DateTime,
    `height`                        Int64,
    `block_part_index`              Int8,
    `tx_index`                      Int32,
    -- msg data
    `msg_part_index`                Int16,
    `msg_indexes`                   Array(Int16),
    -- wasm msg data
    `wasm_part_index`               Int16,
    -- event data
    `msg_part_events_index_offset`  Int32,
    `msg_part_events`               Array(String)
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
    `timestamp`                     DateTime,
    `height`                        Int64,
    `block_part_index`              Int8,
    `tx_index`                      Int32,
    -- msg data
    `msg_part_index`                Int16,
    `msg_indexes`                   Array(Int16),
    -- wasm msg data
    `wasm_part_index`               Int16,
    -- event data
    `msg_part_events_index_offset`  Int32,
    `msg_part_events`               Array(String)
) AS
WITH
    -- note: this is a msg action detection regex, it can determine which Dex v5 msg was used to create this order of events
    --       msgs: https://github.com/neutron-org/neutron/blob/v5.1.3/proto/neutron/dex/tx.proto#L16-L28
    -- test: negative
    --       you can test the coverage of this fingerprinting method by running this SELECT statement with the condition:
    --           WHERE `wasm_part_index` > 0 AND empty(`msg_part_label`)
    --           AND (hasToken(`msg_part_match`, 'TickUpdate') OR hasToken(`msg_part_match`, 'TrancheUserUpdate'))
    --       if any rows are returned, then some TickUpdate or TrancheUserUpdate event exist outside the captured "sub msg" parts
    -- test: positive
    --       you can test that the fingerprint method has detected all msg types correctly by matching the dex action events after WASM dex action events were introduced
    --           WHERE height > 19946990 AND `wasm_part_index` > 0
    --           AND regex_match_label_settings[`msg_part_label`] != arrayReduce('groupUniqArray', arrayMap(
    --               (msg_part_event) -> JSONExtractString(arrayFirst(attr -> JSONExtractString(attr, 'key') = 'action', JSONExtractArrayRaw(msg_part_event, 'attributes')), 'value'),
    --               arrayFilter(msg_part_event -> JSONExtractString(msg_part_event, 'type') = 'message' AND arrayExists(attr -> (JSONExtractString(attr, 'key') = 'module' AND JSONExtractString(attr, 'value') = 'dex'), JSONExtractArrayRaw(msg_part_event, 'attributes')), `msg_part_events`)
    --           ))
    --       if any rows are returned, then some msg_parts have been mis-identified by the fingerprinting method
    concat(
        '(',
        arrayStringConcat(
            [
                -- MsgDeposit
                '(?:execute,)?(?:wasm,)?(?:message,)?(?:(?:neutron,)?(?:(?:neutron,)?TickUpdate,|TickUpdate,(?:neutron,)?)?TickUpdate,)+(?:message,)*(?:coin_spent,coin_received,transfer,(?:message,)?)?coin_spent,coin_received,transfer,(?:message,)?coin_received,coinbase,coin_spent,coin_received,transfer(?:,message)?,',
                -- MsgWithdrawal
                '(?:execute,)?(?:wasm,)?(?:message,)?(?:(?:neutron,)?TickUpdate,)+(?:message,)*coin_spent,coin_received,transfer,(?:message,)?coin_spent,burn(?:,coin_spent,coin_received,transfer(?:,message)?,neutron)+,',
                -- MsgPlaceLimitOrder
                '(?:execute,)?(?:wasm,)?(?:message,)?(?:(?:neutron,)?TickUpdate(?:,TickUpdate)?,)*neutron,(?:neutron,)?(?:TickUpdate,)?TrancheUserUpdate,(?:coin_spent,coin_received,transfer,(?:message,)?)?coin_spent,coin_received,transfer(?:,message)?,',
                -- MsgCancelLimitOrder
                '(?:execute,)?(?:wasm,)?(?:message,)?(?:TrancheUserUpdate,(?:neutron,)?TickUpdate,)+(?:coin_spent,coin_received,transfer,(?:message,)?)?coin_spent,coin_received,transfer(?:,message)?(?:,message)?,',
                -- MsgMultiHopSwap
                '(?:execute,)?(?:wasm,)?(?:message,)?(?:(?:(?:neutron,)?TickUpdate,)+neutron,)+coin_spent,coin_received,transfer,(?:message,)?coin_spent,coin_received,transfer(?:,message)?,',
                -- MsgWithdrawFilledLimitOrder
                '(?:execute,)?(?:wasm,)?(?:message,)?TrancheUserUpdate(?:,coin_spent,coin_received,transfer,(?:message,)?)?,coin_spent,coin_received,transfer(?:,message)?(?:,message)?,',
                -- TrancheExpiration (at end of BeginBlock only, neutron.is_expiring_limit_order = "true")
                '(?:TickUpdate,neutron,)+'
            ],
            ')|('
        ),
        ')'
    ) as regex_string,
    -- the labels for each message part
    map(
        'MsgDeposit', ['DepositLP'],
        'MsgWithdrawal', ['WithdrawLP'],
        'MsgPlaceLimitOrder', ['PlaceLimitOrder'],
        'MsgCancelLimitOrder', ['CancelLimitOrder'],
        'MsgMultiHopSwap', ['MultihopSwap'],
        'MsgWithdrawFilledLimitOrder', ['WithdrawLimitOrder'],
        '(TrancheExpiration)', []
    ) as regex_match_label_settings,
    mapKeys(regex_match_label_settings) as regex_match_labels,
    -- define join tuple parts for row fields
    msg_part_tuple.1 as `msg_part_label`,
    msg_part_tuple.2 as `msg_part_events`,
    msg_part_tuple.3 as `msg_part_event_start_offset`,
    msg_part_tuple.4 as `msg_part_event_end_offset`,
    msg_part_tuple.5 as `msg_part_next_string_match_after_end_position`,
    msg_part_tuple.6 as `msg_part_match`
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `msg_part_index`,
    `msg_indexes`,
    if (
        msg_part_next_string_match_after_end_position > 1,
        -- use 1-based index for existing msg_part splitting
        `msg_part_tuple_index`,
        -- use 0 for a non-split msg_part
        toInt16(0)
    ) as `wasm_part_index`,
    `msg_events_index_offset` + `msg_part_event_start_offset` as `msg_part_events_index_offset`,
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
                        [[[[(
                            -- tuple.1 label
                            '',
                            -- tuple.2 msg_events,
                            `msg_events`,
                            -- tuple.3 msg_event_start_offset (offset starting from 0)
                            toUInt64(0),
                            -- tuple.4 msg_event_end_offset
                            length(`msg_events`),
                            -- tuple.5 next_string_match_after_end_position
                            toUInt64(0),
                            -- tuple.6 msg_part_match
                            ''
                        )]]]],
                        -- compute out all the DEX msg parts of each message
                        arrayMap(
                            (match_groups_array) -> arrayMap(
                                (match_groups_indexes) -> arrayMap(
                                    -- with match group arrays (that can be labelled) get information about each msg part
                                    (match_array, match_label_array) -> arrayFilter(
                                        -- remove empty non-match sections
                                        (tuple) -> notEmpty(tuple.2),
                                        -- reduce through the search string space with a increasing search start position
                                        -- so that the entire search string is searched only once
                                        arrayFold(
                                            (acc, match, match_label, i) -> (
                                                arrayConcat(acc, [
                                                    -- add match result msg_events
                                                    (
                                                        -- tuple.1 label
                                                        match_label,
                                                        -- tuple.2 msg_events
                                                        arraySlice(
                                                            msg_events,
                                                            acc[-1].4 + 1,
                                                            countSubstrings(match, ',') as msg_events_match_length
                                                        ),
                                                        -- tuple.3 msg_event_start_offset
                                                        acc[-1].4,
                                                        -- tuple.4 msg_event_end_offset
                                                        acc[-1].4 + msg_events_match_length as cumulative_msg_event_count,
                                                        -- tuple.5 next_string_match_after_end_position
                                                        acc[-1].5 + length(match),
                                                        -- tuple.6 msg_part_match
                                                        match
                                                    ),
                                                    -- add non-match result msg_events
                                                    (
                                                        -- tuple.1 label
                                                        '',
                                                        -- tuple.2 msg_events
                                                        arraySlice(
                                                            msg_events,
                                                            cumulative_msg_event_count + 1,
                                                            countSubstrings(non_match, ',') as msg_events_non_match_length
                                                        ),
                                                        -- tuple.3 msg_event_start_offset
                                                        cumulative_msg_event_count,
                                                        -- tuple.4 msg_event_end_offset
                                                        cumulative_msg_event_count + msg_events_non_match_length,
                                                        -- tuple.5 next_string_match_after_end_position
                                                        next_string_match_position + length(match_array[i + 1]),
                                                        -- tuple.6 msg_part_match
                                                        substring(
                                                            msg_events__types_string,
                                                            acc[-1].5,
                                                            (
                                                                if (
                                                                    i < length(match_array),
                                                                    -- get start position of next match
                                                                    position(
                                                                        msg_events__types_string,
                                                                        match_array[i + 1],
                                                                        acc[-1].5
                                                                    ),
                                                                    -- get end position of the entire string
                                                                    length(msg_events__types_string) + 1
                                                                ) as next_string_match_position
                                                            ) - acc[-1].5
                                                        ) as non_match
                                                    )
                                                ])
                                            ),
                                            -- "field" match
                                            match_array,
                                            -- "field" match_label
                                            match_label_array,
                                            -- "field" match_index
                                            arrayEnumerate(match_array),
                                            -- initial value tuple
                                            [(
                                                -- tuple.1 label
                                                '',
                                                -- tuple.2 msg_events
                                                arraySlice(
                                                    msg_events,
                                                    1,
                                                    countSubstrings(first_non_match, ',') as msg_event_count
                                                ),
                                                -- tuple.3 msg_event_start_offset (offset starting from 0)
                                                toUInt64(0),
                                                -- tuple.4 msg_event_end_offset (offset starting from 0)
                                                msg_event_count,
                                                -- tuple.5 next_string_match_after_end_position (where should next string search start from)
                                                next_string_match_position + length(match_array[1]),
                                                -- tuple.6 msg_part_match
                                                substring(
                                                    msg_events__types_string,
                                                    1,
                                                    (
                                                        position(
                                                            msg_events__types_string,
                                                            match_array[1]
                                                        ) as next_string_match_position
                                                    ) - 1
                                                ) as first_non_match
                                            )]
                                        )
                                    ),
                                    -- "field" match_array
                                    [arrayMap(
                                        (match_groups, i) -> match_groups[i],
                                        match_groups_array,
                                        match_groups_indexes
                                    )],
                                    -- "field" match_label_array
                                    [arrayMap(
                                        (i) -> regex_match_labels[i],
                                        match_groups_indexes
                                    )]
                                ),
                                [arrayMap(
                                    (match_groups) -> arrayFirstIndex(match -> notEmpty(match), match_groups),
                                    match_groups_array
                                )]
                            ),
                            -- compare tx event types array as string against tx msg detection regex
                            -- to find where the sub-msgs are in each CosmWasm tx `events` list
                            [extractAllGroupsVertical(
                                -- add a comma to the end so counting commas is equivalent to counting events in a "sub msg"
                                concat(arrayStringConcat(msg_events__types, ','), ',') as msg_events__types_string,
                                -- note: this is a msg action detection regex, it can determine which Dex v5 msg was used to create this order of events
                                --       msgs: https://github.com/neutron-org/neutron/blob/v5.1.3/proto/neutron/dex/tx.proto#L16-L28
                                regex_string
                            )]
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
    ) AS `msg_part_tuple`,
    arrayEnumerate(`msg_part_tuple`) as `msg_part_tuple_index`
SETTINGS
    -- split query execution into small chunks to reduce peak memory usage
    max_block_size = 50;
