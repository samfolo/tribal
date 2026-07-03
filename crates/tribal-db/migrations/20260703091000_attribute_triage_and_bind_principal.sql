-- Extend node attribution to the two triage-stage tables — the last record
-- classes that lacked a contributing principal — and make a principal's
-- platform binding a stable, unique key.
--
-- A triage record is attributed to the same principal as the node it concerns.
-- The column is nullable because a record written before a principal resolves
-- carries none.

ALTER TABLE triage_results
    ADD COLUMN principal_id UUID REFERENCES principals(id);

ALTER TABLE triage_similar_item_decisions
    ADD COLUMN principal_id UUID REFERENCES principals(id);

-- One local principal per platform (user, account): the binding-keyed
-- find-or-create resolves to a single row, so two users in one account cannot
-- collapse onto one principal, and a re-resolve is idempotent. Local-only
-- principals (both linkage columns null) are exempt — NULLs compare distinct,
-- so many coexist.
CREATE UNIQUE INDEX principals_platform_binding_unique
    ON principals (platform_user_id, account_reference)
    WHERE platform_user_id IS NOT NULL;
