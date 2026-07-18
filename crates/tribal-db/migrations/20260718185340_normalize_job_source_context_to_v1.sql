-- Normalize recognised flat job source contexts to the typed V1 shape.
--
-- The flat shape carries a PascalCase discriminator with top-level
-- provider/model ("AgentMediated") or a capture_method ("ManualCapture").
-- V1 nests the caller's provider/model under a claimed actor and reserves
-- top-level `extraction` for the binding the engine commits later. Rows
-- whose shape is not recognised are left untouched, and no field a flat
-- row never carried is invented: absent transport stays absent, an empty
-- model is dropped rather than promoted to a claim, and `extraction`
-- starts unset for every migrated row.

UPDATE jobs
SET source_context = jsonb_strip_nulls(
    jsonb_build_object(
        'version', 1,
        'type', CASE source_context->>'type'
            WHEN 'AgentMediated'  THEN 'agent_mediated'
            WHEN 'agent_mediated' THEN 'agent_mediated'
            WHEN 'ManualCapture'  THEN 'manual_capture'
            WHEN 'manual_capture' THEN 'manual_capture'
            WHEN 'Derived'        THEN 'derived'
            WHEN 'derived'        THEN 'derived'
            WHEN 'FileWatch'      THEN 'file_watch'
            WHEN 'file_watch'     THEN 'file_watch'
        END,
        'claimed_actor', CASE
            WHEN NULLIF(source_context->>'provider', '') IS NOT NULL
                OR NULLIF(source_context->>'model', '') IS NOT NULL
            THEN jsonb_build_object(
                'inference', jsonb_strip_nulls(jsonb_build_object(
                    'provider', NULLIF(source_context->>'provider', ''),
                    'model', NULLIF(source_context->>'model', '')
                ))
            )
        END
    )
)
WHERE NOT source_context ? 'version'
  AND source_context->>'type' IN (
      'AgentMediated', 'agent_mediated',
      'ManualCapture', 'manual_capture',
      'Derived', 'derived',
      'FileWatch', 'file_watch'
  );
