use std::collections::BTreeMap;
use std::path::Path;

use super::{code_only, counted, find, line_of, rust_sources, word_byte};

const OPENS_ALLOWED: &str = "bin/nanocode-sim/opens-outside-scopes.txt";
const OPENS_ROOTS: &[&str] = &["crates", "bin", "examples"];
const OPENS_SKIPPED: &[&str] = &[
    "crates/patches",
    "crates/runtime/scope",
    "crates/core/turnlog/src/adopt.rs",
    "crates/core/turnlog/src/parted",
    "bin/nanocode-sim",
];
const SKIPPED_DIRS: &[&str] = &["target", "output", "node_modules"];
const OPENS: &[&str] = &[
    "TurnLog::open(",
    "TurnLog::follow(",
    "OneLog::open(",
    "OneLog::claim(",
    "PartedLog::claim(",
    ".claim(part",
    ".claim(Partition(",
    ".gain(part",
    ".gain(Partition(",
];

pub fn problems(root: &Path) -> std::io::Result<Vec<String>> {
    opens(root)
}

pub fn per_request_opens(code: &[u8]) -> Vec<(usize, String)> {
    let mut opens = Vec::new();
    for open in OPENS {
        let needle = open.as_bytes();
        let mut from = 0;
        while let Some(at) = find(&code[from..], needle) {
            let start = from + at;
            let bounded = needle[0] == b'.' || start == 0 || !word_byte(code[start - 1]);
            if bounded {
                opens.push((line_of(code, start), (*open).to_owned()));
            }
            from = start + needle.len();
        }
    }
    opens.sort();
    opens
}

pub fn open_problems(sources: &[(String, String)], allowed: &str) -> Vec<String> {
    let mut found: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (path, text) in sources {
        for (line, call) in per_request_opens(&code_only(text)) {
            found.entry(path.clone()).or_default().push(format!(
                "{path}:{line}: {call}..) opens, claims or follows a log outside the scope runtime; reach the scope through nanocode_scope::Scopes::get"
            ));
        }
    }
    counted(OPENS_ALLOWED, allowed, &found, "log opens outside the scope runtime")
}

fn opens(root: &Path) -> std::io::Result<Vec<String>> {
    let mut sources = Vec::new();
    for directory in OPENS_ROOTS {
        rust_sources(root, &root.join(directory), OPENS_SKIPPED, SKIPPED_DIRS, &mut sources)?;
    }
    Ok(open_problems(&sources, &std::fs::read_to_string(root.join(OPENS_ALLOWED))?))
}
