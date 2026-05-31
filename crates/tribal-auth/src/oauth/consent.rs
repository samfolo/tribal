//! Consent page for the authorisation endpoint.
//!
//! Renders a self-contained HTML page that displays the redirect URI
//! hostname, the client (its registered name when present, otherwise the
//! opaque identifier), and the effective granted scope, plus a loopback
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
//! This is a display-and-confirm gate, not a server-enforced one. The
//! page is built before the code is persisted, and the code is persisted
//! before the response is returned, so by the time the user can act the
//! code exists server-side; the Authorise anchor is a client-side
//! navigation, so the human click gates delivery of the code to the
//! redirect target but is not itself recorded server-side.
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

/// Consent page model. The template HTML-escapes every interpolated
/// value, and the markup carries no script, so a hostile `client_id`,
/// `client_name`, `scope`, or redirect host cannot break out of its text
/// or attribute context.
#[derive(Template)]
#[template(path = "consent.html")]
struct ConsentPage<'a> {
    target: &'a str,
    redirect_host: &'a str,
    client_id: &'a str,
    client_name: Option<&'a str>,
    scope_display: &'a str,
    is_loopback: bool,
}

/// Builds the consent page HTML.
///
/// `target` is the full redirect URL (with the code and state) the
/// approve anchor points at; `redirect_host` is the redirect authority
/// (host, with its port when non-default) displayed to the user;
/// `is_loopback` selects the loopback warning and is classified by the
/// caller from the bare host; `client_name` is the client's registered
/// display name, shown in place of the opaque `client_id` when present;
/// `scope` is the effective grant the code carries, resolved by the
/// caller. All interpolated values are HTML-escaped.
///
/// # Errors
///
/// Returns an [`askama::Error`] if the template fails to render.
pub fn build_consent_html(
    target: &str,
    redirect_host: &str,
    client_id: &str,
    client_name: Option<&str>,
    scope: &str,
    is_loopback: bool,
) -> Result<String, askama::Error> {
    ConsentPage {
        target,
        redirect_host,
        client_id,
        client_name,
        scope_display: scope,
        is_loopback,
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
            Some("<b>spoofed name</b>"),
            "\"><img src=x onerror=alert(1)>",
            true,
        )
        .expect("template renders");

        // No raw angle bracket from the injected client_id, client_name,
        // or scope survives into the rendered page. The template escaper
        // emits numeric character references (&#60; &#62; &#38;), so the
        // injected markup renders as inert text.
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(!html.contains("<img src=x"));
        assert!(!html.contains("<b>spoofed name</b>"));
        assert!(html.contains("&#60;script&#62;alert(1)&#60;/script&#62;"));
        // The ampersand in the redirect target is attribute-escaped.
        assert!(html.contains("code=abc&#38;state=x"));
    }

    #[test]
    fn test_consent_html_shows_loopback_warning_only_for_loopback() {
        let loopback = build_consent_html(
            "http://127.0.0.1/cb",
            "127.0.0.1",
            "cid",
            None,
            "tribal:read",
            true,
        )
        .expect("template renders");
        assert!(loopback.contains("Loopback redirect"));

        let remote = build_consent_html(
            "https://example.com/cb",
            "example.com",
            "cid",
            None,
            "tribal:read",
            false,
        )
        .expect("template renders");
        assert!(!remote.contains("Loopback redirect"));
    }

    #[test]
    fn test_consent_html_requires_explicit_click_no_auto_advance() {
        // The page must not auto-advance: no script that clicks or
        // submits, and no meta refresh. The user clicks Authorise.
        let html = build_consent_html(
            "http://127.0.0.1/cb?code=abc",
            "127.0.0.1",
            "cid",
            None,
            "tribal:read",
            true,
        )
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
            !html.contains("<form"),
            "consent page must navigate by anchor, never submit a form",
        );
        assert!(
            html.contains(">Authorise</a>"),
            "consent page must offer an explicit Authorise action",
        );
        assert!(
            html.contains("<a class=\"approve\"") && html.contains("href=\""),
            "the Authorise action must be an anchor carrying an href",
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
            "tribal:read",
            false,
        )
        .expect("template renders");
        assert!(!html.contains("<link"), "no external stylesheet");
        assert!(!html.contains("<script"), "no external or inline script");
        assert!(!html.contains("src="), "no external resource reference");
        assert!(
            !html.contains("@import"),
            "no CSS @import of a remote sheet"
        );
        assert!(
            !html.contains("url("),
            "no CSS url() fetch of a font, image, or sheet",
        );
        assert!(
            !html.contains("<iframe") && !html.contains("<object") && !html.contains("<embed"),
            "no embedded external document",
        );
    }

    #[test]
    fn test_consent_html_remote_snapshot() {
        let html = build_consent_html(
            "https://mcp.example.com/callback?code=abc123&state=xyz",
            "mcp.example.com",
            "tribal-cli-7f3a9c2e",
            Some("Tribal CLI"),
            "tribal:read tribal:write",
            false,
        )
        .expect("template renders");
        assert_text_snapshot!(&html, "src/oauth/snapshots/consent-remote.html");
    }

    #[test]
    fn test_consent_html_loopback_snapshot() {
        let html = build_consent_html(
            "http://127.0.0.1:53017/callback?code=abc123&state=xyz",
            "127.0.0.1:53017",
            "s6BhdRkqt3",
            None,
            "tribal:read",
            true,
        )
        .expect("template renders");
        assert_text_snapshot!(&html, "src/oauth/snapshots/consent-loopback.html");
    }

    #[test]
    fn test_consent_html_named_loopback_snapshot() {
        // The highest-value real-world case: a named client (an MCP CLI)
        // connecting to a loopback redirect, so the bdi-wrapped name and the
        // loopback warning appear on the same page.
        let html = build_consent_html(
            "http://127.0.0.1:7777/callback?code=abc123&state=xyz",
            "127.0.0.1:7777",
            "s6BhdRkqt3",
            Some("Local MCP Client"),
            "tribal:read",
            true,
        )
        .expect("template renders");
        assert_text_snapshot!(&html, "src/oauth/snapshots/consent-loopback-named.html");
    }
}
