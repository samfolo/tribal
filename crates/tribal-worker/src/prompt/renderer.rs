//! Prompt template renderer with scoped nonce generation.

use uuid::Uuid;

use super::variables::VAR_NONCE;
use crate::error::StageError;

/// Reserved template variable names injected by the renderer.
///
/// Callers must not set these in externally-built contexts — doing so
/// is a programming error and will panic.
const RESERVED_KEYS: &[&str] = &[VAR_NONCE];

// ---------------------------------------------------------------------------
// PromptRenderer
// ---------------------------------------------------------------------------

/// Renders Tera prompt templates with a scoped content-fence nonce.
///
/// Created once per inference call. The nonce is generated at
/// construction and injected into every context at render time,
/// guaranteeing its presence regardless of what the caller provides.
pub(crate) struct PromptRenderer {
    nonce: String,
}

impl PromptRenderer {
    /// Creates a renderer with a fresh random nonce.
    pub fn new() -> Self {
        Self {
            nonce: Uuid::new_v4().simple().to_string()[..12].to_owned(),
        }
    }

    /// Creates a renderer with a fixed nonce for template validation.
    #[cfg(test)]
    pub fn for_validation() -> Self {
        Self {
            nonce: "validation00".to_owned(),
        }
    }

    /// Renders a template, injecting reserved variables before execution.
    ///
    /// Takes ownership of the caller's context, adds internal defaults
    /// (the nonce), and renders. The nonce is harmlessly present even
    /// for system prompts whose templates do not reference it.
    ///
    /// # Panics
    ///
    /// Panics if the context contains a reserved key. This is a
    /// programming error — reserved keys are managed by the renderer.
    pub fn render(
        &self,
        template: &str,
        mut context: tera::Context,
        description: &'static str,
    ) -> Result<String, StageError> {
        for key in RESERVED_KEYS {
            assert!(
                context.get(key).is_none(),
                "reserved template variable '{key}' must not be set externally",
            );
        }
        context.insert(VAR_NONCE, &self.nonce);

        tera::Tera::one_off(template, &context, false).map_err(|source| {
            StageError::TemplateRender {
                context: description.into(),
                source,
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nonce_is_12_hex_chars() {
        let renderer = PromptRenderer::new();
        let result = renderer
            .render("{{ nonce }}", tera::Context::new(), "test")
            .unwrap();
        assert_eq!(result.len(), 12);
        assert!(
            result.chars().all(|c| c.is_ascii_hexdigit()),
            "nonce should be hex: {result}",
        );
    }

    #[test]
    fn test_validation_renderer_has_stable_nonce() {
        let renderer = PromptRenderer::for_validation();
        let result = renderer
            .render("{{ nonce }}", tera::Context::new(), "test")
            .unwrap();
        assert_eq!(result, "validation00");
    }

    #[test]
    fn test_render_merges_nonce_with_caller_context() {
        let renderer = PromptRenderer::new();
        let mut ctx = tera::Context::new();
        ctx.insert("name", "world");
        let result = renderer
            .render("{{ name }} {{ nonce }}", ctx, "test")
            .unwrap();
        assert!(result.starts_with("world "));
        assert_eq!(result.split(' ').count(), 2);
    }

    #[test]
    fn test_render_returns_error_for_invalid_template() {
        let renderer = PromptRenderer::new();
        let result = renderer.render("{{ x | bogus }}", tera::Context::new(), "test");
        assert!(result.is_err());
    }

    #[test]
    #[should_panic(expected = "reserved template variable")]
    fn test_render_panics_on_reserved_key() {
        let renderer = PromptRenderer::new();
        let mut ctx = tera::Context::new();
        ctx.insert("nonce", "sneaky");
        let _ = renderer.render("{{ nonce }}", ctx, "test");
    }
}
