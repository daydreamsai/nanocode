use std::{collections::BTreeMap, path::Path};

use crate::tidy::code_only;

const ROOT: &str = "crates";
const SKIPPED: &[&str] = &["crates/patches"];
const SKIPPED_DIRS: &[&str] = &["target", "output", "node_modules"];
const TASK_LOCAL: &[u8] = b"task_local!";
const SCOPE_RUNTIME: &str = "crates/runtime/scope/src/";
const METER: &str = "crates/runtime/scope/src/meter.rs";
const UNMETERED: &[&str] = &["log", "host", "checkpoints"];
const HOST_CALLS: &[&str] = &[
    "scan",
    "scans",
    "scan_ranges",
    "load",
    "append",
    "append_compacting",
    "extend",
    "carry",
    "checkpoint",
    "save_checkpoint",
    "latest",
    "parts",
    "write",
];
const SEMAPHORE: &[u8] = b"Semaphore::new(";
const LITERALS_ALLOWED: &str = "bin/nanocode-sim/semaphores-literal.txt";

const fn word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn line_of(code: &[u8], at: usize) -> usize {
    code[..at].iter().filter(|byte| **byte == b'\n').count() + 1
}

fn positions(code: &[u8], needle: &[u8]) -> Vec<usize> {
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(at) = code[from..]
        .windows(needle.len())
        .position(|window| window == needle)
    {
        found.push(from + at);
        from += at + needle.len();
    }
    found
}

fn sources(
    root: &Path,
    directory: &Path,
    found: &mut Vec<(String, String)>,
) -> std::io::Result<()> {
    let mut entries = std::fs::read_dir(directory)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    entries.sort();
    for path in entries {
        let relative = path
            .strip_prefix(root)
            .expect("a scanned file lies under the workspace root")
            .to_string_lossy()
            .into_owned();
        let skipped = SKIPPED.contains(&relative.as_str())
            || path
                .file_name()
                .is_some_and(|name| SKIPPED_DIRS.iter().any(|skipped| name == *skipped));
        if skipped {
            continue;
        }
        if path.is_dir() {
            sources(root, &path, found)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push((relative, std::fs::read_to_string(&path)?));
        }
    }
    Ok(())
}

pub fn task_locals(path: &str, code: &[u8]) -> Vec<String> {
    positions(code, TASK_LOCAL)
        .into_iter()
        .filter(|at| *at == 0 || !word_byte(code[at - 1]))
        .map(|at| {
            format!(
                "{path}:{}: task_local! carries state that work on another task never sees; pass it as an argument (the scope runtime's meter attributes database work by the scope each call names)",
                line_of(code, at)
            )
        })
        .collect()
}

fn receiver(code: &[u8], dot: usize) -> &[u8] {
    let mut end = dot;
    while end > 0 && code[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    let mut start = end;
    while start > 0 && word_byte(code[start - 1]) {
        start -= 1;
    }
    &code[start..end]
}

fn called(code: &[u8], after: usize, method: &[u8]) -> bool {
    let Some(rest) = code.get(after..) else {
        return false;
    };
    let Some(rest) = rest.trim_ascii_start().strip_prefix(method) else {
        return false;
    };
    !rest.first().copied().is_some_and(word_byte) && rest.trim_ascii_start().starts_with(b"(")
}

pub fn unmetered(path: &str, code: &[u8]) -> Vec<String> {
    let mut problems = Vec::new();
    for dot in positions(code, b".") {
        let receiver = String::from_utf8_lossy(receiver(code, dot)).into_owned();
        if !UNMETERED.contains(&receiver.as_str()) {
            continue;
        }
        for method in HOST_CALLS {
            if called(code, dot + 1, method.as_bytes()) {
                problems.push(format!(
                    "{path}:{}: {receiver}.{method}(..) reaches the host past the meter; call it through the metered views in meter.rs (logs.load, logs.append, logs.adopt, logs.history, meter.latest, meter.parts, meter.write) so the scope it names is counted",
                    line_of(code, dot)
                ));
            }
        }
    }
    problems
}

fn literal(argument: &[u8]) -> bool {
    let argument = argument.trim_ascii();
    let digits = argument.first().is_some_and(u8::is_ascii_digit)
        && argument.iter().all(|byte| word_byte(*byte));
    let constant = argument.first().is_some_and(u8::is_ascii_uppercase)
        && argument
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || *byte == b'_');
    digits || constant
}

pub fn literal_capacities(code: &[u8]) -> Vec<(usize, String)> {
    let mut found = Vec::new();
    for at in positions(code, SEMAPHORE) {
        if at > 0 && word_byte(code[at - 1]) && code[at - 1] != b':' {
            continue;
        }
        let open = at + SEMAPHORE.len();
        let Some(close) = code[open..].iter().position(|byte| *byte == b')') else {
            continue;
        };
        let argument = &code[open..open + close];
        if literal(argument) {
            let shown = String::from_utf8_lossy(argument.trim_ascii()).into_owned();
            found.push((line_of(code, at), shown));
        }
    }
    found
}

fn allowed(listed: &str, problems: &mut Vec<String>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for line in listed.lines().filter(|line| !line.trim().is_empty()) {
        match line
            .rsplit_once(' ')
            .map(|(path, count)| (path, count.parse::<usize>()))
        {
            Some((path, Ok(count))) if count > 0 => {
                counts.insert(path.to_owned(), count);
            }
            _ => problems.push(format!(
                "{LITERALS_ALLOWED}: '{line}' is not '<path> <count above zero>'"
            )),
        }
    }
    counts
}

pub fn literal_problems(sources: &[(String, String)], listed: &str) -> Vec<String> {
    let mut problems = Vec::new();
    let counts = allowed(listed, &mut problems);
    let mut found: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for (path, text) in sources {
        for (line, argument) in literal_capacities(&code_only(text)) {
            found.entry(path).or_default().push(format!(
                "{path}:{line}: Semaphore::new({argument}) takes a literal capacity; size it from what it bounds (a pool's connections, a host's read_capacity, caller policy)"
            ));
        }
    }
    for (path, found) in &found {
        let allowed = counts.get(*path).copied().unwrap_or(0);
        if found.len() > allowed {
            problems.extend(found.iter().map(|problem| {
                format!(
                    "{problem} ({} of {allowed} allowed in {LITERALS_ALLOWED})",
                    found.len()
                )
            }));
        }
    }
    for (path, allowed) in &counts {
        let count = found.get(path.as_str()).map_or(0, Vec::len);
        if count < *allowed {
            problems.push(format!(
                "{LITERALS_ALLOWED}: {path} has {count} literal Semaphore capacities where it allows {allowed}; lower it to {count}"
            ));
        }
    }
    problems
}

pub fn check(root: &Path) -> std::io::Result<Vec<String>> {
    let mut found = Vec::new();
    sources(root, &root.join(ROOT), &mut found)?;
    let mut problems = Vec::new();
    for (path, text) in &found {
        let code = code_only(text);
        problems.extend(task_locals(path, &code));
        if path.starts_with(SCOPE_RUNTIME) && path != METER {
            problems.extend(unmetered(path, &code));
        }
    }
    let listed = std::fs::read_to_string(root.join(LITERALS_ALLOWED))?;
    problems.extend(literal_problems(&found, &listed));
    Ok(problems)
}
