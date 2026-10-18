//! How weft spells a name, one convention per layer: PascalCase for node
//! types and custom types; camelCase for node names, ports, settings,
//! record fields, `metadata.json` keys and the JSON fields weft produces;
//! snake_case for the values of a fixed set (statuses, kinds), `.weft`
//! files and folders, `weft.toml` keys and SQL; kebab-case for CLI
//! commands and flags; SCREAMING_SNAKE_CASE for environment variables.
//!
//! The catalog test and the compiler's casing warning both ask these
//! functions, so a name passes or fails the same way wherever it is read.

/// One spelling convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Casing {
    /// `GetRunId`: starts upper case, letters and digits.
    Pascal,
    /// `maxInboundMb`: starts lower case, letters and digits.
    Camel,
    /// `waiting_for_input`: lower case words joined by `_`.
    Snake,
    /// `running-policy`: lower case words joined by `-`.
    Kebab,
    /// `WEFT_INSTALL`: upper case words joined by `_`.
    ScreamingSnake,
}

impl Casing {
    /// Whether `name` is spelled this way. An empty name never is.
    pub fn holds(self, name: &str) -> bool {
        let mut chars = name.chars();
        let Some(first) = chars.next() else { return false };
        match self {
            Casing::Pascal => first.is_ascii_uppercase() && name.chars().all(|c| c.is_ascii_alphanumeric()),
            Casing::Camel => first.is_ascii_lowercase() && name.chars().all(|c| c.is_ascii_alphanumeric()),
            Casing::Snake => words(name, '_', |c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            Casing::Kebab => words(name, '-', |c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            Casing::ScreamingSnake => words(name, '_', |c| c.is_ascii_uppercase() || c.is_ascii_digit()),
        }
    }

    /// The convention's name as a message says it.
    pub fn label(self) -> &'static str {
        match self {
            Casing::Pascal => "PascalCase",
            Casing::Camel => "camelCase",
            Casing::Snake => "snake_case",
            Casing::Kebab => "kebab-case",
            Casing::ScreamingSnake => "SCREAMING_SNAKE_CASE",
        }
    }

    /// `name` spelled this way: its words (split at `_`, `-`, spaces and
    /// at each lower-to-upper step) joined the way this convention joins
    /// them. What a message offers as the fix.
    pub fn spell(self, name: &str) -> String {
        let words = split_words(name);
        let capitalized = |w: &str| {
            let mut chars = w.chars();
            chars.next().map(|c| c.to_ascii_uppercase().to_string() + chars.as_str()).unwrap_or_default()
        };
        match self {
            Casing::Pascal => words.iter().map(|w| capitalized(w)).collect(),
            Casing::Camel => words
                .iter()
                .enumerate()
                .map(|(i, w)| if i == 0 { w.clone() } else { capitalized(w) })
                .collect(),
            Casing::Snake => words.join("_"),
            Casing::Kebab => words.join("-"),
            Casing::ScreamingSnake => words.join("_").to_ascii_uppercase(),
        }
    }

    /// `Ok` when `name` is spelled this way; otherwise what is wrong and
    /// the spelling that would fit, for `what` (a "port", a "node type").
    pub fn check(self, what: &str, name: &str) -> Result<(), String> {
        if self.holds(name) {
            return Ok(());
        }
        let fix = self.spell(name);
        if fix.is_empty() || !self.holds(&fix) {
            return Err(format!("{what} '{name}' is not {}", self.label()));
        }
        Err(format!("{what} '{name}' is not {}: write '{fix}'", self.label()))
    }
}

/// Whether `name` is non-empty words of `allowed` characters joined by
/// single `sep`s, starting with a letter.
fn words(name: &str, sep: char, allowed: impl Fn(char) -> bool) -> bool {
    name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        && name.split(sep).all(|w| !w.is_empty() && w.chars().all(&allowed))
}

/// `name`'s words in lower case. A word starts at `_`, `-` or a space,
/// at each lower-to-upper step, and at the last capital of a run of
/// capitals that a lower case letter follows (`XMLHttp` is `xml`,
/// `http`).
fn split_words(name: &str) -> Vec<String> {
    let chars: Vec<char> = name.chars().collect();
    let mut out: Vec<String> = Vec::new();
    let mut word = String::new();
    for (i, &c) in chars.iter().enumerate() {
        if c == '_' || c == '-' || c == ' ' {
            if !word.is_empty() {
                out.push(std::mem::take(&mut word));
            }
            continue;
        }
        let prev = i.checked_sub(1).map(|j| chars[j]);
        let next = chars.get(i + 1).copied();
        let after_lower = prev.is_some_and(|p| p.is_ascii_lowercase() || p.is_ascii_digit());
        let ends_acronym = prev.is_some_and(|p| p.is_ascii_uppercase()) && next.is_some_and(|n| n.is_ascii_lowercase());
        if c.is_ascii_uppercase() && (after_lower || ends_acronym) && !word.is_empty() {
            out.push(std::mem::take(&mut word));
        }
        word.push(c.to_ascii_lowercase());
    }
    if !word.is_empty() {
        out.push(word);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::Casing::*;

    #[test]
    fn pascal_case_is_an_upper_case_start_and_letters_and_digits() {
        for ok in ["GetRunId", "ListRuns", "Signup", "Http2Request", "LLM"] {
            assert!(Pascal.holds(ok), "{ok}");
        }
        for bad in ["getRunId", "Get_Run", "Get-Run", "", "2Fast"] {
            assert!(!Pascal.holds(bad), "{bad}");
        }
    }

    #[test]
    fn camel_case_is_a_lower_case_start_and_letters_and_digits() {
        for ok in ["runId", "maxInboundMb", "keepRunsFor", "hitBody", "pick", "top10", "h264"] {
            assert!(Camel.holds(ok), "{ok}");
        }
        for bad in ["ttl_days", "smtp_host", "RunId", "run-id", "", "_error", "9lives"] {
            assert!(!Camel.holds(bad), "{bad}");
        }
    }

    #[test]
    fn snake_kebab_and_screaming_join_lower_or_upper_words() {
        assert!(Snake.holds("waiting_for_input") && Snake.holds("completed") && Snake.holds("keyframes_phase"));
        assert!(!Snake.holds("waitingForInput") && !Snake.holds("double__sep") && !Snake.holds("trailing_"));
        assert!(Kebab.holds("running-policy") && Kebab.holds("which") && !Kebab.holds("running_policy"));
        assert!(ScreamingSnake.holds("WEFT_INSTALL") && !ScreamingSnake.holds("Weft_Install"));
    }

    #[test]
    fn a_name_is_respelled_word_by_word() {
        assert_eq!(Camel.spell("smtp_host"), "smtpHost");
        assert_eq!(Camel.spell("access_key_id"), "accessKeyId");
        assert_eq!(Camel.spell("default_art"), "defaultArt");
        assert_eq!(Pascal.spell("get_run_id"), "GetRunId");
        assert_eq!(Snake.spell("waitingForInput"), "waiting_for_input");
        assert_eq!(Kebab.spell("runningPolicy"), "running-policy");
        assert_eq!(ScreamingSnake.spell("weftInstall"), "WEFT_INSTALL");
    }

    #[test]
    fn a_run_of_capitals_ends_before_the_capital_that_starts_the_next_word() {
        assert_eq!(Camel.spell("XMLHttp_request"), "xmlHttpRequest");
        assert_eq!(Pascal.spell("HTTPServer_x"), "HttpServerX");
        assert_eq!(Camel.spell("parse_URL"), "parseUrl");
        assert_eq!(Camel.spell("LLM_model"), "llmModel");
        assert_eq!(Snake.spell("getHTTPResponse"), "get_http_response");
        assert_eq!(Camel.check("port", "XMLHttp_request").unwrap_err(), "port 'XMLHttp_request' is not camelCase: write 'xmlHttpRequest'");
    }

    #[test]
    fn a_check_names_the_fix() {
        assert_eq!(Camel.check("port", "runId"), Ok(()));
        assert_eq!(Camel.check("port", "ttl_days").unwrap_err(), "port 'ttl_days' is not camelCase: write 'ttlDays'");
        assert_eq!(Pascal.check("node type", "get_run_id").unwrap_err(), "node type 'get_run_id' is not PascalCase: write 'GetRunId'");
        assert_eq!(Camel.check("port", "_x").unwrap_err(), "port '_x' is not camelCase: write 'x'");
    }
}
