//! Prompt template renderer with scoped nonce generation.

use strum::IntoEnumIterator;
use uuid::Uuid;

use super::variables::ReservedVariable;
use crate::error::StageError;

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
    pub(crate) fn new() -> Self {
        Self {
            nonce: Uuid::new_v4().simple().to_string()[..12].to_owned(),
        }
    }

    /// Creates a renderer with fixed values for template validation.
    #[cfg(test)]
    pub fn for_validation() -> Self {
        Self {
            nonce: ReservedVariable::Nonce.validation_default().to_owned(),
        }
    }

    /// Renders a template, injecting reserved variables before execution.
    ///
    /// Takes ownership of the caller's context, adds internal defaults
    /// (the nonce), and renders. The nonce is harmlessly present even
    /// for system prompts whose templates do not reference it.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::TemplateRender`] if the context contains a
    /// reserved key or the template cannot be rendered.
    pub(crate) fn render(
        &self,
        template: &str,
        mut context: tera::Context,
        description: &'static str,
    ) -> Result<String, StageError> {
        for var in ReservedVariable::iter() {
            if context.get(var.key()).is_some() {
                return Err(StageError::TemplateRender {
                    context: format!(
                        "reserved template variable '{}' must not be set externally",
                        var.key(),
                    ),
                    source: tera::Error::msg(format!(
                        "context already contains reserved key '{}'",
                        var.key(),
                    )),
                });
            }
        }
        self.inject_reserved(&mut context);

        tera::Tera::one_off(template, &context, false).map_err(|source| {
            StageError::TemplateRender {
                context: description.into(),
                source,
            }
        })
    }

    /// Injects production values for all reserved variables.
    ///
    /// Exhaustive match ensures a new variant forces a wiring update.
    fn inject_reserved(&self, ctx: &mut tera::Context) {
        for var in ReservedVariable::iter() {
            match var {
                ReservedVariable::Nonce => ctx.insert(var.key(), &self.nonce),
            }
        }
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
        assert_eq!(result, ReservedVariable::Nonce.validation_default());
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
    fn test_render_rejects_reserved_key() {
        let renderer = PromptRenderer::new();
        let mut ctx = tera::Context::new();
        ctx.insert("nonce", "sneaky");
        let result = renderer.render("{{ nonce }}", ctx, "test");
        assert!(result.is_err());
    }
}
