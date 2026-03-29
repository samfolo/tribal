//! Prompt template renderer with scoped nonce generation.

use strum::IntoEnumIterator;
use uuid::Uuid;

use super::variables::VAR_NONCE;
use crate::error::StageError;

// ---------------------------------------------------------------------------
// ReservedVariable
// ---------------------------------------------------------------------------

/// Template variables managed exclusively by the renderer.
///
/// Adding a new variant automatically propagates to the reserved-key
/// panic check in `render()`, validation default injection, and the
/// coverage test — all driven by `strum::IntoEnumIterator`.
#[derive(Debug, Clone, Copy, strum::EnumIter)]
enum ReservedVariable {
    Nonce,
}

impl ReservedVariable {
    /// The Tera context key for this variable.
    fn key(self) -> &'static str {
        match self {
            Self::Nonce => VAR_NONCE,
        }
    }

    /// The fixed value used in validation and test contexts.
    fn validation_default(self) -> &'static str {
        match self {
            Self::Nonce => "validation00",
        }
    }
}

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

    /// Creates a renderer with fixed values for template validation.
    #[cfg(test)]
    pub fn for_validation() -> Self {
        Self {
            nonce: ReservedVariable::Nonce.validation_default().to_owned(),
        }
    }

    /// Injects validation-safe values for all reserved variables.
    ///
    /// Used by `synthetic_validation_context` to ensure the server's
    /// hot-reload validator sees the same variable set that production
    /// rendering provides.
    pub(crate) fn inject_validation_defaults(ctx: &mut tera::Context) {
        for var in ReservedVariable::iter() {
            ctx.insert(var.key(), var.validation_default());
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
        for var in ReservedVariable::iter() {
            assert!(
                context.get(var.key()).is_none(),
                "reserved template variable '{}' must not be set externally",
                var.key(),
            );
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
    fn test_validation_defaults_cover_all_reserved_variables() {
        let mut ctx = tera::Context::new();
        PromptRenderer::inject_validation_defaults(&mut ctx);
        for var in ReservedVariable::iter() {
            assert!(
                ctx.get(var.key()).is_some(),
                "reserved variable '{var:?}' missing from validation defaults",
            );
        }
    }

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
    #[should_panic(expected = "reserved template variable")]
    fn test_render_panics_on_reserved_key() {
        let renderer = PromptRenderer::new();
        let mut ctx = tera::Context::new();
        ctx.insert("nonce", "sneaky");
        let _ = renderer.render("{{ nonce }}", ctx, "test");
    }
}
