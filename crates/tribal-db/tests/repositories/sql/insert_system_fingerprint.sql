INSERT INTO system_fingerprints
    (content_hash, build_version, extraction_binding_hash, triage_binding_hash,
     relation_binding_hash, embedding_provider, embedding_model, embedding_dimensions,
     pipeline_parameters)
VALUES ($1, 'migration', $2, $2, $2, 'openai', 'text-embedding-3-small', 1536, '{}')
