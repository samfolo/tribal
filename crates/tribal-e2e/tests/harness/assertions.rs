/// Asserts that a tool call result indicates success.
///
/// On failure, includes the error content for immediate diagnosis.
macro_rules! assert_success {
    ($result:expr) => {{
        let result = &$result;
        assert!(
            !result.is_error.unwrap_or(false),
            "expected tool success, got error: {}",
            $crate::harness::tool_call::tool_result_text(result),
        );
    }};
}

pub(crate) use assert_success;
