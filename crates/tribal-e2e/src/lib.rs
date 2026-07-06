//! End-to-end test infrastructure for the Tribal MCP server.
//!
//! # Fixture theme: Canopy
//!
//! All E2E tests use a shared fictional setting: **Meridian Labs**, a
//! startup building a real-time collaboration platform called
//! **Canopy**. Test fixtures model the engineering knowledge that a
//! team accumulates while building and operating the platform —
//! architecture decisions, operational heuristics, debugging
//! procedures, and technical facts.
//!
//! This gives the test suite semantic coherence: knowledge items
//! reference Canopy subsystems (event replay, session cache,
//! deployment pipeline, notification fan-out), and relation graphs
//! reflect how engineering understanding evolves over time
//! (supersession, supporting evidence, contradictions).
