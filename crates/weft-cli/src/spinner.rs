//! A line per long wait going on, saying it is moving, the way
//! `./setup.sh` says it:
//!
//!   ⠹ media: building image weft-infra-studio (2m14s) #19 79.27 ...... [ 55%]
//!   ⠹ feeds: starting infra db (12s)
//!
//! redrawn in place on stderr while the waits last: what is going on, for
//! how long, and the last line of its log when that log is a file on this
//! machine. Lines that stay ([`Spinner::say`]) print above them. Off a
//! terminal (a script, a CI log) nothing is redrawn: each new line is said
//! once, then again each minute it lasts, so a log never goes silent.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const FRAMES: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
const DRAW_EVERY: Duration = Duration::from_millis(100);
/// Off a terminal, how often a line that lasts is said again.
const SAY_AGAIN_EVERY: i64 = 60;

/// See the module doc. Stops drawing, and clears its lines, when dropped.
pub struct Spinner {
    block: Arc<Mutex<Block>>,
    ticker: tokio::task::JoinHandle<()>,
}

/// One wait going on: what it is, since when, and its log when it has one.
pub struct Shown {
    pub label: String,
    pub since_unix: i64,
    pub log: Option<String>,
}

struct Block {
    tty: bool,
    shown: Vec<Shown>,
    /// How many lines are on screen now.
    drawn: usize,
    frame: usize,
    /// Off a terminal: when each line shown was last said.
    said_unix: BTreeMap<String, i64>,
}

impl Spinner {
    pub fn start() -> Self {
        let block = Arc::new(Mutex::new(Block {
            tty: std::io::stderr().is_terminal(),
            shown: Vec::new(),
            drawn: 0,
            frame: 0,
            said_unix: BTreeMap::new(),
        }));
        let ticking = block.clone();
        let ticker = tokio::spawn(async move {
            let mut tick = tokio::time::interval(DRAW_EVERY);
            loop {
                tick.tick().await;
                if let Ok(mut block) = ticking.lock() {
                    block.draw();
                }
            }
        });
        Self { block, ticker }
    }

    /// Show one wait, going on since `since_unix`.
    pub fn show_one(&self, label: impl Into<String>, since_unix: i64) {
        self.show(vec![Shown { label: label.into(), since_unix, log: None }]);
    }

    /// Show these waits, replacing the ones shown (none clears them).
    pub fn show(&self, shown: Vec<Shown>) {
        let Ok(mut block) = self.block.lock() else { return };
        block.said_unix.retain(|label, _| shown.iter().any(|now| &now.label == label));
        block.shown = shown;
        block.draw();
    }

    /// A line that stays, printed above the spinner (on stderr when
    /// `err`, else stdout).
    pub fn say(&self, text: &str, err: bool) {
        let Ok(mut block) = self.block.lock() else { return };
        block.erase();
        if err {
            eprintln!("{text}");
        } else {
            println!("{text}");
        }
    }
}

impl Drop for Spinner {
    fn drop(&mut self) {
        self.ticker.abort();
        if let Ok(mut block) = self.block.lock() {
            block.erase();
        }
    }
}

impl Block {
    fn draw(&mut self) {
        let now = crate::progress::now_unix() as i64;
        let elapsed = |since: i64| crate::progress::elapsed_text(now.saturating_sub(since).max(0) as u64);
        if !self.tty {
            for shown in &self.shown {
                let said = self.said_unix.entry(shown.label.clone()).or_insert(i64::MIN);
                if now.saturating_sub(*said) >= SAY_AGAIN_EVERY {
                    *said = now;
                    eprintln!("  {} ({})", shown.label, elapsed(shown.since_unix));
                }
            }
            return;
        }
        let frame = FRAMES[self.frame % FRAMES.len()];
        self.frame += 1;
        let width = terminal_size::terminal_size().map(|(w, _)| w.0 as usize).unwrap_or(80).saturating_sub(1);
        let lines: Vec<String> = self
            .shown
            .iter()
            .map(|shown| {
                let head = format!("{frame} {} ({})", shown.label, elapsed(shown.since_unix));
                let tail = shown.log.as_deref().map(Path::new).filter(|path| path.is_absolute()).and_then(last_line).unwrap_or_default();
                let text = if tail.is_empty() { head } else { format!("{head} \x1b[2m{tail}") };
                format!("{}\x1b[0m", cut(&text, width))
            })
            .collect();
        self.erase();
        if lines.is_empty() {
            return;
        }
        let mut err = std::io::stderr().lock();
        let _ = write!(err, "{}", lines.join("\n"));
        let _ = err.flush();
        self.drawn = lines.len();
    }

    /// Take the lines on screen away, leaving the cursor where the first
    /// one began.
    fn erase(&mut self) {
        if self.drawn == 0 {
            return;
        }
        let mut err = std::io::stderr().lock();
        let _ = write!(err, "\r\x1b[K{}", "\x1b[1A\x1b[K".repeat(self.drawn - 1));
        let _ = err.flush();
        self.drawn = 0;
    }
}

/// `text` cut to `width` characters on screen, the escape codes it
/// carries not counted.
fn cut(text: &str, width: usize) -> String {
    let mut out = String::new();
    let mut seen = 0;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            out.push(c);
            for c in chars.by_ref() {
                out.push(c);
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
            continue;
        }
        if seen == width {
            break;
        }
        out.push(c);
        seen += 1;
    }
    out
}

/// The last line `log` holds with something on it, its escape codes and
/// runs of spaces dropped. A progress bar rewrites its line with `\r`, so
/// that counts as a line's end.
fn last_line(log: &Path) -> Option<String> {
    if !log.is_file() {
        return None;
    }
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(log).ok()?;
    let len = file.metadata().ok()?.len();
    file.seek(SeekFrom::Start(len.saturating_sub(4096))).ok()?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).ok()?;
    last_line_of(&String::from_utf8_lossy(&bytes))
}

fn last_line_of(text: &str) -> Option<String> {
    let line = text.split(['\n', '\r']).rev().map(plain).find(|line| !line.is_empty())?;
    Some(line)
}

/// `line` without its escape codes, its runs of spaces made one.
fn plain(line: &str) -> String {
    let mut out = String::new();
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            chars.by_ref().find(|c| c.is_ascii_alphabetic());
            continue;
        }
        out.push(if c.is_whitespace() { ' ' } else { c });
    }
    out.split(' ').filter(|word| !word.is_empty()).collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_last_line_with_something_on_it_is_shown_plain() {
        assert_eq!(last_line_of("#19 1.6 ....\n#19 79.27 ....  [ 55%]\n\n").as_deref(), Some("#19 79.27 .... [ 55%]"));
        assert_eq!(last_line_of("Downloading 10%\rDownloading 60%\r").as_deref(), Some("Downloading 60%"), "a bar's last state");
        assert_eq!(last_line_of("\x1b[32mok\x1b[0m done\n").as_deref(), Some("ok done"));
        assert_eq!(last_line_of("\n \n"), None);
    }

    #[test]
    fn a_line_is_cut_to_the_screen_not_counting_its_colours() {
        assert_eq!(cut("abcdef", 3), "abc");
        assert_eq!(cut("ab\x1b[2mcdef", 3), "ab\x1b[2mc");
        assert_eq!(cut("⠹ é", 10), "⠹ é");
    }
}
