//! Consent page for the authorisation endpoint.
//!
//! Renders a minimal HTML page that displays the redirect URI hostname,
//! the client identifier, and the requested scope, plus a loopback
//! warning, and requires the user to click Authorise to release the
//! authorisation code to the redirect target.
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
//! Navigation is an anchor whose `href` is the full redirect target,
//! carrying the `code` and `state`. A GET form would rebuild its query
//! string from input fields and strip those parameters; the anchor
//! follows the `href` byte-for-byte.

use crate::oauth::common::is_loopback_host;

/// Placeholder shown when the client requested no explicit scope.
const NO_SCOPE_REQUESTED: &str = "(no explicit scope requested)";

/// Warning rendered for loopback redirect hosts.
const LOOPBACK_WARNING: &str = "<p>This is a loopback redirect. Any process listening on this host can receive the code; only proceed if you started a local client expecting it.</p>";

/// Builds the consent page HTML.
///
/// `target` is the full redirect URL (with the code and state) the
/// approve anchor points at; `redirect_host` is the redirect URI host
/// displayed to the user and checked against the loopback list. All
/// interpolated values are HTML-escaped.
#[must_use]
pub fn build_consent_html(
    target: &str,
    redirect_host: &str,
    client_id: &str,
    scope: Option<&str>,
) -> String {
    let scope_display = scope.unwrap_or(NO_SCOPE_REQUESTED);
    let warning_html = if is_loopback_host(redirect_host) {
        LOOPBACK_WARNING
    } else {
        ""
    };
    let target_html = escape_html_attribute(target);
    let client_id_html = escape_html_text(client_id);
    let redirect_host_html = escape_html_text(redirect_host);
    let scope_html = escape_html_text(scope_display);
    format!(
        "<!doctype html>
<html><head><meta charset=\"utf-8\"><title>Tribal authorisation</title></head>
<body>
  <h1>Authorisation request</h1>
  <p><strong>client_id</strong>: <code>{client_id_html}</code></p>
  <p><strong>redirect_uri host</strong>: <code>{redirect_host_html}</code></p>
  <p><strong>requested scope</strong>: <code>{scope_html}</code></p>
  {warning_html}
  <p><a id=\"approve\" href=\"{target_html}\">Authorise</a></p>
</body></html>
",
    )
}

/// HTML-escapes a value for use inside a double-quoted attribute.
fn escape_html_attribute(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// HTML-escapes a value for use inside element text content.
fn escape_html_text(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consent_html_escapes_interpolated_values() {
        let html = build_consent_html(
            "http://127.0.0.1:9000/cb?code=abc&state=x",
            "127.0.0.1",
            "<script>alert(1)</script>",
            Some("\"><img src=x onerror=alert(1)>"),
        );

        // No raw angle bracket from the injected client_id or scope
        // survives into the rendered page.
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(!html.contains("<img src=x"));
        assert!(html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
        // The ampersand in the redirect target is attribute-escaped.
        assert!(html.contains("code=abc&amp;state=x"));
    }

    #[test]
    fn test_consent_html_shows_loopback_warning_only_for_loopback() {
        let loopback = build_consent_html("http://127.0.0.1/cb", "127.0.0.1", "cid", None);
        assert!(loopback.contains("loopback redirect"));

        let remote = build_consent_html("https://example.com/cb", "example.com", "cid", None);
        assert!(!remote.contains("loopback redirect"));
    }

    #[test]
    fn test_consent_html_requires_explicit_click_no_auto_advance() {
        // The page must not auto-advance: no script that clicks or
        // submits, and no meta refresh. The user clicks Authorise.
        let html = build_consent_html("http://127.0.0.1/cb?code=abc", "127.0.0.1", "cid", None);
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
}
