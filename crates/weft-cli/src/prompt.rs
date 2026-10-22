//! Interactive prompts shared by the destructive / choice-bearing CLI
//! verbs. ONE rule, enforced in one place: every command must be fully
//! runnable via flags, and when the deciding flag is absent we either
//! ask a human (a real terminal) or, if there is no human (piped /
//! redirected stdin, an AI or a script), error LOUDLY naming the flag,
//! never hang on a read that nobody will answer and never silently pick
//! a default.

use std::io::{IsTerminal, Write};

/// Is there a human at every end? Prompting needs stdin, stdout AND
/// stderr on a terminal. The question rides stderr (so a `--json`
/// run's stdout stays one JSON object per line even when a prompt
/// fires), but the CONTEXT the question refers to (a numbered
/// connection list, a consent URL) prints on stdout, so a redirected
/// stdout would leave the user answering a question whose list went
/// into a file; and a read from a pipe blocks on nobody. THE one
/// definition of "interactive"; every prompt and every
/// prompt-or-default decision reads it, never a bare `is_terminal()`
/// probe of its own.
pub fn is_interactive() -> bool {
    std::io::stdin().is_terminal()
        && std::io::stdout().is_terminal()
        && std::io::stderr().is_terminal()
}

/// Read one line from the user for `prompt`. On a terminal: print the
/// prompt to stderr, flush, read a trimmed line. With NO terminal
/// (piped/closed stdin or stderr): bail naming `flag_hint`, the flag(s) that choose
/// this non-interactively, so a scripted/AI run fails fast with a fix
/// instead of blocking forever. The caller interprets the returned line
/// (yes/no, a number, a menu choice); this owns only the terminal gate
/// + read.
pub fn prompt_line(prompt: &str, flag_hint: &str) -> anyhow::Result<String> {
    if !is_interactive() {
        // Name the failing end: a run with a piped output stream reads
        // fine but the prompt (stderr) or its context (stdout) would
        // vanish into the pipe.
        let why = if !std::io::stdin().is_terminal() {
            "no terminal to read this prompt's answer from"
        } else if !std::io::stdout().is_terminal() {
            "stdout is not a terminal, so what the prompt refers to would be invisible"
        } else {
            "stderr is not a terminal, so the prompt would be invisible"
        };
        anyhow::bail!("{why}; pass {flag_hint} to choose non-interactively");
    }
    eprint!("{prompt}");
    std::io::stderr().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(line.trim().to_string())
}

/// Yes/no confirmation for a destructive verb. `true` only on an
/// explicit "yes"/"y" (any casing); anything else (including a bare
/// Enter or EOF) is the safe answer: no. Non-interactive stdin bails via
/// [`prompt_line`] naming `flag_hint`.
pub fn confirm(prompt: &str, flag_hint: &str) -> anyhow::Result<bool> {
    let answer = prompt_line(prompt, flag_hint)?;
    Ok(matches!(answer.to_lowercase().as_str(), "yes" | "y"))
}

/// A yes/no question that is not destructive, so a bare Enter (or EOF)
/// takes `default`. Anything other than y/yes/n/no asks again.
/// Non-interactive stdin bails via [`prompt_line`] naming `flag_hint`.
pub fn ask_yes_no(prompt: &str, default: bool, flag_hint: &str) -> anyhow::Result<bool> {
    loop {
        match parse_yes_no(&prompt_line(prompt, flag_hint)?, default) {
            Some(answer) => return Ok(answer),
            None => eprintln!("answer y or n"),
        }
    }
}

fn parse_yes_no(answer: &str, default: bool) -> Option<bool> {
    match answer.to_lowercase().as_str() {
        "" => Some(default),
        "y" | "yes" => Some(true),
        "n" | "no" => Some(false),
        _ => None,
    }
}

/// Pick one or more of `count` entries the caller already printed as a
/// list numbered from 1. Answers are numbers separated by spaces or
/// commas; a wrong one is explained and asked again. Returns zero-based
/// indices in the order given, each once. An empty answer (or EOF) picks
/// nothing, which is an error naming `flag_hint`: the caller asked
/// because a choice is required. Non-interactive stdin bails via
/// [`prompt_line`].
pub fn pick_numbers(prompt: &str, count: usize, flag_hint: &str) -> anyhow::Result<Vec<usize>> {
    loop {
        let answer = prompt_line(prompt, flag_hint)?;
        if answer.is_empty() {
            anyhow::bail!("nothing was picked; pass {flag_hint} to choose without being asked");
        }
        match parse_numbers(&answer, count) {
            Ok(picked) => return Ok(picked),
            Err(why) => eprintln!("{why}"),
        }
    }
}

fn parse_numbers(answer: &str, count: usize) -> Result<Vec<usize>, String> {
    let mut picked = Vec::new();
    for word in answer.split(|c: char| c == ',' || c.is_whitespace()).filter(|w| !w.is_empty()) {
        let index = word
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=count).contains(n))
            .ok_or_else(|| format!("'{word}' is not a number between 1 and {count}"))?;
        if !picked.contains(&(index - 1)) {
            picked.push(index - 1);
        }
    }
    Ok(picked)
}

#[cfg(test)]
mod tests {
    use super::{parse_numbers, parse_yes_no};

    #[test]
    fn numbers_split_on_spaces_and_commas_and_come_back_zero_based_once_each() {
        assert_eq!(parse_numbers("2", 3), Ok(vec![1]));
        assert_eq!(parse_numbers("3, 1 3", 3), Ok(vec![2, 0]));
    }

    #[test]
    fn a_number_off_the_list_is_refused_with_the_range() {
        assert_eq!(parse_numbers("0", 3), Err("'0' is not a number between 1 and 3".into()));
        assert_eq!(parse_numbers("1 x", 3), Err("'x' is not a number between 1 and 3".into()));
    }

    #[test]
    fn a_bare_enter_takes_the_default_and_a_stray_word_asks_again() {
        assert_eq!(parse_yes_no("", true), Some(true));
        assert_eq!(parse_yes_no("N", true), Some(false));
        assert_eq!(parse_yes_no("yes", false), Some(true));
        assert_eq!(parse_yes_no("maybe", true), None);
    }
}
