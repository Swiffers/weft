//! A folder on the machine the install runs on: fires a run each time a
//! file in it appears, changes or goes. The listener watches it with the
//! operating system's change events, so it is held (`BetweenFires::Holds`)
//! by a holder that runs on that same machine: a local install's own
//! process. A cloud's holders run on machines of their own, where the
//! person's folders are not, and the listener refuses the signal there.
//!
//! What a fire carries is [`FolderChange`]: the file's absolute path, what
//! happened to it, and its size when it is still there. The worker runs on
//! the same machine and opens the path itself.

use serde::{Deserialize, Serialize};

use super::Signal;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FolderWatch {
    /// The folder to watch, absolute. The listener does not know where a
    /// project's folder is, so a relative one is refused.
    pub folder: String,
    /// Watch the folders under it too. Off: only the files directly in it.
    #[serde(default)]
    pub recursive: bool,
    /// Only files whose NAME matches this glob ([`NamePattern`]). `None`:
    /// every file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    /// Which changes fire. Every one unless the author narrowed it.
    #[serde(default = "FolderWatch::every_event")]
    pub events: Vec<FolderEvent>,
}

/// What happened to a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FolderEvent {
    /// A file is there that was not (written, copied or moved in).
    Created,
    /// A file that was there has another size or modification time.
    Changed,
    /// A file that was there is not any more (deleted or moved out).
    Removed,
}

impl FolderEvent {
    pub const ALL: [FolderEvent; 3] = [FolderEvent::Created, FolderEvent::Changed, FolderEvent::Removed];

    /// The word a fire and a node's setting carry.
    pub fn as_str(self) -> &'static str {
        match self {
            FolderEvent::Created => "created",
            FolderEvent::Changed => "changed",
            FolderEvent::Removed => "removed",
        }
    }

    /// The event a word names, or what the words are.
    pub fn parse(word: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|e| e.as_str() == word)
            .ok_or_else(|| format!("'{word}' is not a folder event; the events are created, changed and removed"))
    }
}

/// What one fire of a folder watch carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FolderChange {
    /// The file's absolute path.
    pub path: String,
    pub event: FolderEvent,
    /// The file's size in bytes; absent for a removed file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

impl FolderWatch {
    /// The events a watch fires on when the author named none: all of
    /// them. An empty list is not that: it is refused ([`Signal::validate`]).
    pub fn every_event() -> Vec<FolderEvent> {
        FolderEvent::ALL.to_vec()
    }

    /// The name pattern, parsed; `None` when every file counts.
    pub fn name_pattern(&self) -> Result<Option<NamePattern>, String> {
        self.pattern.as_deref().map(NamePattern::parse).transpose()
    }

    /// Whether a change of this kind fires.
    pub fn fires_on(&self, event: FolderEvent) -> bool {
        self.events.contains(&event)
    }
}

impl Signal for FolderWatch {
    const TAG: &'static str = "folder_watch";

    /// The shape only. Whether the folder is there, and whether this
    /// install can watch it at all, are the machine's questions: the
    /// listener answers them where the folder is.
    fn validate(&self) -> Result<(), String> {
        let folder = self.folder.as_str();
        if folder.is_empty() {
            return Err("folder_watch.folder must not be empty".into());
        }
        if !std::path::Path::new(folder).is_absolute() {
            return Err(format!(
                "folder_watch.folder must be an absolute path (one starting with `/`), got '{folder}'; \
                 the listener does not know where the project's folder is, so it cannot resolve a relative one"
            ));
        }
        if self.events.is_empty() {
            return Err("folder_watch.events is empty, so nothing would ever fire; name at least one of created, changed, removed".into());
        }
        self.name_pattern()?;
        Ok(())
    }
}

/// A glob over a file's name (never its folder): `*` is any run of
/// characters, `?` one character, `[abc]`, `[a-z]` and `[!abc]` one
/// character of a set or outside it, and `{jpg,png}` one of several
/// spellings (`*.{jpg,png}`). Everything else stands for itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamePattern {
    /// One sequence per spelling the braces make; a name matches when any
    /// of them does.
    spellings: Vec<Vec<Piece>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Piece {
    Char(char),
    One,
    Run,
    Set { negated: bool, ranges: Vec<(char, char)> },
}

impl NamePattern {
    pub fn parse(pattern: &str) -> Result<Self, String> {
        let bad = |why: &str| format!("folder_watch.pattern '{pattern}' {why}");
        if pattern.is_empty() {
            return Err(bad("is empty; leave the pattern out to match every file"));
        }
        if pattern.contains('/') {
            return Err(bad("holds a `/`, but it matches a file's name only; set `recursive` to look in the folders under it"));
        }
        let mut spellings = Vec::new();
        for spelling in expand_braces(pattern).map_err(|why| bad(&why))? {
            spellings.push(parse_pieces(&spelling).map_err(|why| bad(&why))?);
        }
        Ok(Self { spellings })
    }

    pub fn matches(&self, name: &str) -> bool {
        let name: Vec<char> = name.chars().collect();
        self.spellings.iter().any(|pieces| matches_from(pieces, &name))
    }
}

/// Every spelling the `{a,b}` groups make. Groups do not nest: a brace
/// inside a brace is refused.
fn expand_braces(pattern: &str) -> Result<Vec<String>, String> {
    let Some(open) = pattern.find('{') else {
        if pattern.contains('}') {
            return Err("closes a `}` it never opened".into());
        }
        return Ok(vec![pattern.to_string()]);
    };
    let rest = &pattern[open + 1..];
    let close = rest.find('}').ok_or("opens a `{` it never closes")?;
    let inside = &rest[..close];
    if inside.contains('{') {
        return Err("puts a `{` inside another, which is not supported".into());
    }
    let (before, after) = (&pattern[..open], &rest[close + 1..]);
    let mut out = Vec::new();
    for tail in expand_braces(after)? {
        for choice in inside.split(',') {
            out.push(format!("{before}{choice}{tail}"));
        }
    }
    Ok(out)
}

fn parse_pieces(spelling: &str) -> Result<Vec<Piece>, String> {
    let mut pieces = Vec::new();
    let mut chars = spelling.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '*' => {
                // Two stars in a row match what one does.
                if pieces.last() != Some(&Piece::Run) {
                    pieces.push(Piece::Run);
                }
            }
            '?' => pieces.push(Piece::One),
            '[' => {
                let negated = chars.next_if(|&c| c == '!').is_some();
                let mut ranges = Vec::new();
                let mut closed = false;
                while let Some(c) = chars.next() {
                    // A `]` first in the set is the character itself.
                    if c == ']' && !ranges.is_empty() {
                        closed = true;
                        break;
                    }
                    if chars.peek() == Some(&'-') {
                        chars.next();
                        match chars.next() {
                            Some(']') | None => {
                                // `[a-]`: the `-` is a character.
                                ranges.push((c, c));
                                ranges.push(('-', '-'));
                                closed = true;
                                break;
                            }
                            Some(end) if end < c => return Err(format!("has the range `{c}-{end}`, which runs backwards")),
                            Some(end) => ranges.push((c, end)),
                        }
                    } else {
                        ranges.push((c, c));
                    }
                }
                if !closed {
                    return Err("opens a `[` it never closes".into());
                }
                pieces.push(Piece::Set { negated, ranges });
            }
            ']' => return Err("closes a `]` it never opened".into()),
            c => pieces.push(Piece::Char(c)),
        }
    }
    Ok(pieces)
}

/// Whether `name` is spelled by `pieces`, in time proportional to the two
/// lengths multiplied, whatever the pattern. Every piece but a `*` takes
/// exactly one character, so when the name stops fitting, the only choice
/// left to change is how much the last `*` passed took: one character
/// more, then go on from there. An earlier `*` never needs to take more,
/// since the last one can take anything it would have.
fn matches_from(pieces: &[Piece], name: &[char]) -> bool {
    let (mut p, mut n) = (0, 0);
    // The last `*` passed, and where in the name the pieces after it start.
    let mut star: Option<(usize, usize)> = None;
    while n < name.len() {
        match pieces.get(p) {
            Some(Piece::Run) => {
                star = Some((p, n));
                p += 1;
            }
            Some(piece) if fits(piece, name[n]) => {
                p += 1;
                n += 1;
            }
            _ => match star {
                Some((at, from)) => {
                    star = Some((at, from + 1));
                    p = at + 1;
                    n = from + 1;
                }
                None => return false,
            },
        }
    }
    pieces[p..].iter().all(|piece| *piece == Piece::Run)
}

/// Whether the character `c` is one `piece` takes. A `*` is never asked.
fn fits(piece: &Piece, c: char) -> bool {
    match piece {
        Piece::Char(want) => c == *want,
        Piece::One => true,
        Piece::Set { negated, ranges } => ranges.iter().any(|&(lo, hi)| (lo..=hi).contains(&c)) != *negated,
        Piece::Run => unreachable!("a `*` is handled by the matcher"),
    }
}

crate::register_signal_kind!(FolderWatch);

#[cfg(test)]
mod tests {
    use super::*;

    fn watch(folder: &str) -> FolderWatch {
        FolderWatch { folder: folder.into(), recursive: false, pattern: None, events: FolderWatch::every_event() }
    }

    #[test]
    fn a_relative_folder_is_refused_with_why() {
        let err = watch("inbox").validate().unwrap_err();
        assert!(err.contains("absolute") && err.contains("inbox"), "{err}");
        assert!(watch("").validate().unwrap_err().contains("must not be empty"));
        watch("/home/q/inbox").validate().expect("an absolute folder is fine");
    }

    #[test]
    fn no_event_at_all_is_refused() {
        let mut w = watch("/in");
        w.events.clear();
        assert!(w.validate().unwrap_err().contains("nothing would ever fire"));
    }

    #[test]
    fn a_bad_pattern_is_refused_at_registration() {
        for (pattern, says) in [
            ("a/*.txt", "name only"),
            ("[abc", "never closes"),
            ("*.{jpg", "never closes"),
            ("x}", "never opened"),
            ("[z-a]", "backwards"),
            ("{a,{b}}", "inside another"),
            ("", "is empty"),
        ] {
            let mut w = watch("/in");
            w.pattern = Some(pattern.into());
            let err = w.validate().unwrap_err();
            assert!(err.contains(says), "{pattern}: {err}");
        }
    }

    #[test]
    fn the_config_reads_with_its_defaults() {
        let w: FolderWatch = serde_json::from_value(serde_json::json!({ "folder": "/in" })).unwrap();
        assert_eq!(w, watch("/in"), "not recursive, every file, every event");
        let spec = crate::signal::to_spec(FolderWatch { pattern: Some("*.pdf".into()), ..watch("/in") });
        assert_eq!(spec.kind, "folder_watch");
        assert_eq!(spec.config["events"], serde_json::json!(["created", "changed", "removed"]));
        crate::signal::validate_spec(&spec).expect("the wire shape validates");
    }

    #[test]
    fn a_fire_speaks_camel_case_and_drops_the_size_of_a_removed_file() {
        let created = FolderChange { path: "/in/a.pdf".into(), event: FolderEvent::Created, size_bytes: Some(12) };
        assert_eq!(
            serde_json::to_value(&created).unwrap(),
            serde_json::json!({ "path": "/in/a.pdf", "event": "created", "sizeBytes": 12 })
        );
        let removed = FolderChange { path: "/in/a.pdf".into(), event: FolderEvent::Removed, size_bytes: None };
        assert_eq!(serde_json::to_value(&removed).unwrap(), serde_json::json!({ "path": "/in/a.pdf", "event": "removed" }));
    }

    #[test]
    fn an_event_word_round_trips() {
        for e in FolderEvent::ALL {
            assert_eq!(FolderEvent::parse(e.as_str()), Ok(e));
        }
        assert!(FolderEvent::parse("moved").unwrap_err().contains("created, changed and removed"));
    }

    #[test]
    fn a_pattern_matches_names() {
        let p = |s: &str| NamePattern::parse(s).unwrap();
        assert!(p("*.pdf").matches("report.pdf"));
        assert!(p("*.pdf").matches(".pdf"));
        assert!(!p("*.pdf").matches("report.pdf.part"));
        assert!(p("*.{jpg,png}").matches("cat.png"));
        assert!(p("*.{jpg,png}").matches("cat.jpg"));
        assert!(!p("*.{jpg,png}").matches("cat.gif"));
        assert!(p("scan-??.tif").matches("scan-07.tif"));
        assert!(!p("scan-??.tif").matches("scan-7.tif"));
        assert!(p("[a-c]*").matches("bob"));
        assert!(!p("[a-c]*").matches("dan"));
        assert!(p("[!.]*").matches("notes.txt"));
        assert!(!p("[!.]*").matches(".hidden"));
        assert!(p("[]x]").matches("]"));
        assert!(p("a[x-]").matches("a-"));
        assert!(p("**.md").matches("x.md"));
        assert!(p("é*").matches("été.txt"), "a character, never a byte");
        assert!(p("{a,b}-{1,2}").matches("b-1"));
        assert!(!p("{a,b}-{1,2}").matches("c-1"));
        assert!(p("{,x}y").matches("y"), "an empty choice is a spelling");
        assert!(p("*").matches(""));
        assert!(p("a*b*c").matches("axxbyyc"));
        assert!(p("a*b*c").matches("abbc"), "a later `*` takes what an earlier match could have");
        assert!(!p("a*b*c").matches("axxbyy"));
        assert!(p("*ab").matches("aab"), "a `*` gives back a character it took");
        assert!(!p("?*?").matches("a"));
    }

    /// Many stars against a long name that almost matches: a matcher that
    /// tries every split for each `*` never finishes here, and this one
    /// takes at most the pattern's length times the name's steps.
    #[test]
    fn a_pattern_full_of_stars_matches_in_linear_steps() {
        let pattern = NamePattern::parse("*a*a*a*a*a*a*a*a*a*a*a*a*b").unwrap();
        let name = "a".repeat(5000);
        let started = std::time::Instant::now();
        assert!(!pattern.matches(&name));
        assert!(pattern.matches(&format!("{name}b")));
        assert!(started.elapsed() < std::time::Duration::from_secs(1), "took {:?}", started.elapsed());
    }
}
