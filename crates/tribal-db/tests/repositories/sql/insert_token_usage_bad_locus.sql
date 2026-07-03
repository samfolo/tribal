-- An out-of-vocabulary execution_locus, to prove the CHECK refuses it. The
-- triage stage takes no purpose, so this row is otherwise valid.
INSERT INTO token_usage
    (stage, provider, model, tokens_input, tokens_output, tokens_total,
     latency_ms, execution_locus)
VALUES ('triage', 'p', 'm', 1, 0, 1, 1, 'frobnicate')
