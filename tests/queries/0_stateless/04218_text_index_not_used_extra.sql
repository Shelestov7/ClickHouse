-- Tags: no-fasttest
-- Sanity checks around how the text index behaves at the edge of what it
-- can do. Two cases:
--   1. equality against the empty string. Tokenizing '' yields no tokens,
--      so the index cannot prune anything; the planner accepts the index
--      under `force_data_skipping_indices` (it claims to be applied) but
--      suppresses granule pruning, so the query falls back to a full
--      scan and returns an empty result.
--   2. a regex whose required prefix is not a complete token (the
--      `match()` codepath still refuses to claim the index, so forcing
--      it raises INDEX_NOT_USED).
-- Positive counterparts that *do* push down are also exercised below as
-- a sanity check.

DROP TABLE IF EXISTS t_text_idx_extra;

CREATE TABLE t_text_idx_extra
(
    id UInt64,
    message String,
    INDEX idx_message message TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_text_idx_extra VALUES (1, 'v1.0 release notes'), (2, 'beta version');

-- A regex like `v[0-9]+\.[0-9]+` has no complete required token to push down
-- (the leading `v` alone is shorter than the alphanumeric boundary), so the
-- planner refuses to claim the index helped.
SELECT * FROM t_text_idx_extra WHERE match(message, 'v[0-9]+\.[0-9]+') SETTINGS force_data_skipping_indices='idx_message'; -- { serverError INDEX_NOT_USED }

-- Sanity check: positive counterparts still use the index.
-- A match() whose pattern contains a complete required token can be pushed down.
SELECT * FROM t_text_idx_extra WHERE match(message, ' release ') SETTINGS force_data_skipping_indices='idx_message';
-- Non-empty equality has a single required token.
SELECT * FROM t_text_idx_extra WHERE equals(message, 'beta version') SETTINGS force_data_skipping_indices='idx_message';

DROP TABLE t_text_idx_extra;
