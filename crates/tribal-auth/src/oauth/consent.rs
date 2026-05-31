//! Consent page for the authorisation endpoint.
//!
//! Renders a self-contained HTML page that displays the redirect URI
//! hostname, the client identifier, and the requested scope, plus a
//! loopback warning, and requires the user to click Authorise to release
//! the authorisation code to the redirect target.
//!
//! The page clearly displays the redirect URI hostname and requires an
//! explicit human action rather than auto-advancing (MCP 2025-11-25,
//! Localhost Redirect URI Risks and Open Redirection). A crafted
//! `/authorize` URL naming an attacker-controlled loopback redirect
//! would otherwise deliver a code to a listening local process the
//! instant the page loaded in a JS-enabled browser, before the user
//! could perceive the destination; the code is released only after the
//! user confirms the hostname shown.
//!
//! This is a display-and-confirm gate, not a server-enforced one. The
//! code is persisted before the page renders and the Authorise anchor is
//! a client-side navigation, so the human click gates delivery of the
//! code to the redirect target but is not itself recorded server-side.
//! Its security value is that a human perceives the redirect host before
//! any code reaches it, which is what the loopback threat model requires:
//! a single-user local server where the agent acts on the user's own
//! behalf. A networked or multi-party authorisation server instead
//! requires a server-enforced confirmation (a POST back to the server
//! that releases the code only on explicit approval); the code-issue
//! point moves behind that POST, leaving the rest of this flow intact.
//!
//! Navigation is an anchor whose `href` is the full redirect target,
//! carrying the `code` and `state`. A GET form would rebuild its query
//! string from input fields and strip those parameters; the anchor
//! follows the `href` byte-for-byte.

use askama::Template;

use crate::oauth::common::is_loopback_host;

/// Placeholder shown when the client requested no explicit scope.
const NO_SCOPE_REQUESTED: &str = "(no explicit scope requested)";

/// Consent page model. The template HTML-escapes every interpolated
/// value, and the markup carries no script, so a hostile `client_id`,
/// `scope`, or redirect host cannot break out of its text or attribute
/// context.
#[derive(Template)]
#[template(path = "consent.html")]
struct ConsentPage<'a> {
    target: &'a str,
    redirect_host: &'a str,
    client_id: &'a str,
    scope_display: &'a str,
    is_loopback: bool,
}

/// Builds the consent page HTML.
///
/// `target` is the full redirect URL (with the code and state) the
/// approve anchor points at; `redirect_host` is the redirect URI host
/// displayed to the user and checked against the loopback list. All
/// interpolated values are HTML-escaped.
///
/// # Errors
///
/// Returns an [`askama::Error`] if the template fails to render.
pub fn build_consent_html(
    target: &str,
    redirect_host: &str,
    client_id: &str,
    scope: Option<&str>,
) -> Result<String, askama::Error> {
    ConsentPage {
        target,
        redirect_host,
        client_id,
        scope_display: scope.unwrap_or(NO_SCOPE_REQUESTED),
        is_loopback: is_loopback_host(redirect_host),
    }
    .render()
}

#[cfg(test)]
mod tests {
    use tribal_test_utils::assert_text_snapshot;

    use super::*;

    #[test]
    fn test_consent_html_escapes_interpolated_values() {
        let html = build_consent_html(
            "http://127.0.0.1:9000/cb?code=abc&state=x",
            "127.0.0.1",
            "<script>alert(1)</script>",
            Some("\"><img src=x onerror=alert(1)>"),
        )
        .expect("template renders");

        // No raw angle bracket from the injected client_id or scope
        // survives into the rendered page. The template escaper emits
        // numeric character references (&#60; &#62; &#38;), so the
        // injected markup renders as inert text.
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(!html.contains("<img src=x"));
        assert!(html.contains("&#60;script&#62;alert(1)&#60;/script&#62;"));
        // The ampersand in the redirect target is attribute-escaped.
        assert!(html.contains("code=abc&#38;state=x"));
    }

    #[test]
    fn test_consent_html_shows_loopback_warning_only_for_loopback() {
        let loopback = build_consent_html("http://127.0.0.1/cb", "127.0.0.1", "cid", None)
            .expect("template renders");
        assert!(loopback.contains("Loopback redirect"));

        let remote = build_consent_html("https://example.com/cb", "example.com", "cid", None)
            .expect("template renders");
        assert!(!remote.contains("Loopback redirect"));
    }

    #[test]
    fn test_consent_html_requires_explicit_click_no_auto_advance() {
        // The page must not auto-advance: no script that clicks or
        // submits, and no meta refresh. The user clicks Authorise.
        let html = build_consent_html("http://127.0.0.1/cb?code=abc", "127.0.0.1", "cid", None)
            .expect("template renders");
        assert!(
            !html.contains("<script"),
            "consent page must carry no script"
        );
        assert!(
            !html.contains(".click("),
            "consent page must not auto-click"
        );
        assert!(
            !html.contains("http-equiv=\"refresh\""),
            "consent page must not auto-refresh",
        );
        assert!(
            html.contains(">Authorise</a>"),
            "consent page must offer an explicit Authorise action",
        );
    }

    #[test]
    fn test_consent_html_makes_no_external_request() {
        // The page is self-contained: styles are inline and nothing is
        // fetched, so it has no external attack or tracking surface.
        let html = build_consent_html(
            "https://mcp.example.com/cb?code=abc",
            "mcp.example.com",
            "cid",
            None,
        )
        .expect("template renders");
        assert!(!html.contains("<link"), "no external stylesheet");
        assert!(!html.contains("<script"), "no external or inline script");
        assert!(!html.contains("src="), "no external resource reference");
    }

    #[test]
    fn test_consent_html_remote_snapshot() {
        let html = build_consent_html(
            "https://mcp.example.com/callback?code=abc123&state=xyz",
            "mcp.example.com",
            "tribal-cli-7f3a9c2e",
            Some("tribal.memory:read tribal.memory:write"),
        )
        .expect("template renders");
        assert_text_snapshot!(&html, "src/oauth/snapshots/consent-remote.html");
    }

    #[test]
    fn test_consent_html_loopback_snapshot() {
        let html = build_consent_html(
            "http://127.0.0.1:53017/callback?code=abc123&state=xyz",
            "127.0.0.1",
            "s6BhdRkqt3",
            Some("tribal.memory:read"),
        )
        .expect("template renders");
        assert_text_snapshot!(&html, "src/oauth/snapshots/consent-loopback.html");
    }
}
