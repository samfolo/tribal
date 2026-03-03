-- Add justification column to knowledge_item_relations for recording the
-- relation agent's reasoning when creating a relationship edge.  Nullable
-- because not every LLM response includes a justification.

ALTER TABLE knowledge_item_relations ADD COLUMN justification TEXT;
