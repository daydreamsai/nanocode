use std::path::Path;

use crate::tidy::{code_only, rust_sources};

const JOB_ROOTS: &[&str] = &["crates/core/turnlog/src", "crates/runtime/scope/src"];
const ROWS_ROOT: &str = "crates";
const SKIPPED: &[&str] = &["crates/patches"];
const SKIPPED_DIRS: &[&str] = &["target", "output", "node_modules"];
const CLOSED: &str = ".closed()";
const ABANDON: &str = "Mutation::AbandonRunningJob";
const CLOSED_ALLOWED: usize = 1;
const SAME_ROWS: &str = "same_rows(";
const DEFINED: &str = "fn same_rows(";
const STAMPS: &str = "crates/core/turnlog/src/stamps.rs";
const FALLBACK: &str = "fn unstamped(";
const OWN_ROW_FENCES: &str = "Mutation::OwnRowFences";
const SAME_ROWS_ALLOWED: usize = 2;

fn sources(root: &Path, directories: &[&str]) -> std::io::Result<Vec<(String, String)>> {
    let mut found = Vec::new();
    for directory in directories {
        rust_sources(
            root,
            &root.join(directory),
            SKIPPED,
            SKIPPED_DIRS,
            &mut found,
        )?;
    }
    Ok(found)
}

fn guard<'a>(lines: &[&'a str], at: usize) -> Option<&'a str> {
    lines[..at]
        .iter()
        .rev()
        .find(|line| line.contains("if ") || line.contains("fn "))
        .copied()
}

fn uses(text: &str, needle: &str) -> (Vec<String>, Vec<usize>) {
    let code = String::from_utf8_lossy(&code_only(text)).into_owned();
    let lines: Vec<String> = code.lines().map(str::to_owned).collect();
    let found = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.contains(needle))
        .map(|(at, _)| at)
        .collect();
    (lines, found)
}

fn closed(root: &Path) -> std::io::Result<Vec<String>> {
    let mut problems = Vec::new();
    let mut allowed = 0;
    for (path, text) in sources(root, JOB_ROOTS)? {
        let (lines, found) = uses(&text, CLOSED);
        let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
        for at in found {
            if guard(&lines, at).is_some_and(|line| line.contains(ABANDON)) {
                allowed += 1;
            } else {
                problems.push(format!(
                    "{path}:{}: races a job against its reply's closed(); a writer job that started runs to its answer and records its outcome, and only the reply is dropped (rule 9)",
                    at + 1
                ));
            }
        }
    }
    if allowed != CLOSED_ALLOWED {
        problems.push(format!(
            "{}: {allowed} jobs race their reply's closed() under {ABANDON} where the abandon_running_job mutation makes {CLOSED_ALLOWED}",
            JOB_ROOTS.join(", ")
        ));
    }
    Ok(problems)
}

fn compared(root: &Path) -> std::io::Result<Vec<String>> {
    let mut problems = Vec::new();
    let mut allowed = 0;
    for (path, text) in sources(root, &[ROWS_ROOT])? {
        let (lines, found) = uses(&text, SAME_ROWS);
        let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
        for at in found {
            if lines[at].contains(DEFINED) {
                continue;
            }
            let fallback = path == STAMPS
                && guard(&lines, at)
                    .is_some_and(|line| line.contains(FALLBACK) || line.contains(OWN_ROW_FENCES));
            if fallback {
                allowed += 1;
            } else {
                problems.push(format!(
                    "{path}:{}: compares rows by body (same_rows) outside the format 1 fallback of read-back in {STAMPS}; read-back decides by stamp (rule 9)",
                    at + 1
                ));
            }
        }
    }
    if allowed != SAME_ROWS_ALLOWED {
        problems.push(format!(
            "{STAMPS}: {allowed} body comparisons where the format 1 fallback and the own_row_fences mutation make {SAME_ROWS_ALLOWED}"
        ));
    }
    Ok(problems)
}

pub fn check(root: &Path) -> std::io::Result<Vec<String>> {
    let mut problems = closed(root)?;
    problems.extend(compared(root)?);
    Ok(problems)
}
