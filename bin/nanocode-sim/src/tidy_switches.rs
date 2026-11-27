use std::path::Path;

const REGISTRY: &str = "crates/runtime/scope/src/switches.rs";
const FLIPS: &str = "bin/nanocode-sim/src/flips.rs";
const DRIVERS: &str = "const DRIVEN:";
const SWITCH: &str = "OnDisk::new(";
const PROOF: &str = "Flipped::by(";
const SCENARIO: &str = "flips";
const LISTED: &str = "pub const ON_DISK:";
const ROOTS: &[&str] = &["crates", "bin", "examples"];
const SKIPPED: &[&str] = &["bin/nanocode-sim", "crates/patches"];
const SKIPPED_DIRS: &[&str] = &["target", "output", "node_modules"];

struct Switch {
    constant: String,
    name: String,
    scenario: String,
}

fn quoted(text: &str) -> Option<(&str, &str)> {
    let (_, rest) = text.split_once('"')?;
    rest.split_once('"')
}

fn constant(before: &str) -> Option<String> {
    let (_, named) = before.rsplit_once("const ")?;
    let (name, _) = named.split_once(':')?;
    Some(name.trim().to_owned())
}

fn switches(registry: &str) -> Vec<Result<Switch, String>> {
    let mut pieces = registry.split(SWITCH);
    let mut before = pieces.next().unwrap_or_default();
    let mut found = Vec::new();
    for after in pieces {
        let name = quoted(after).map(|(name, _)| name.to_owned());
        let scenario = after
            .split_once(PROOF)
            .and_then(|(_, proof)| quoted(proof))
            .map(|(scenario, _)| scenario.to_owned());
        found.push(match (constant(before), name, scenario) {
            (Some(constant), Some(name), Some(scenario)) => Ok(Switch {
                constant,
                name,
                scenario,
            }),
            _ => Err(format!(
                "{REGISTRY}: an on-disk switch is not a named const with a name and a Flipped::by proof"
            )),
        });
        before = after;
    }
    found
}

fn listed(registry: &str) -> &str {
    registry
        .split_once(LISTED)
        .and_then(|(_, list)| list.split_once("];"))
        .map_or("", |(list, _)| list)
}

fn driven(flips: &str) -> &str {
    flips
        .split_once(DRIVERS)
        .and_then(|(_, drivers)| drivers.split_once("];"))
        .map_or("", |(drivers, _)| drivers)
}

fn outside(root: &Path) -> std::io::Result<Vec<String>> {
    let mut sources = Vec::new();
    for directory in ROOTS {
        crate::tidy::rust_sources(
            root,
            &root.join(directory),
            SKIPPED,
            SKIPPED_DIRS,
            &mut sources,
        )?;
    }
    let mut problems = Vec::new();
    for (path, text) in sources {
        if path != REGISTRY && text.contains(SWITCH) {
            problems.push(format!(
                "{path}: builds an on-disk switch outside the one registry in {REGISTRY}"
            ));
        }
    }
    Ok(problems)
}

pub fn check(root: &Path) -> std::io::Result<Vec<String>> {
    let registry = std::fs::read_to_string(root.join(REGISTRY))?;
    let flips = std::fs::read_to_string(root.join(FLIPS))?;
    let (listed, driven) = (listed(&registry), driven(&flips));
    let mut problems = outside(root)?;
    for switch in switches(&registry) {
        let switch = match switch {
            Ok(switch) => switch,
            Err(problem) => {
                problems.push(problem);
                continue;
            }
        };
        if !listed.contains(&switch.constant) {
            problems.push(format!(
                "{REGISTRY}: on-disk switch {} is not listed in ON_DISK",
                switch.constant
            ));
        }
        if switch.scenario != SCENARIO || !driven.contains(&format!("\"{}\"", switch.name)) {
            problems.push(format!(
                "{REGISTRY}: on-disk switch \"{}\" is not driven by --scenario {SCENARIO}: prove it with Flipped::by(\"{SCENARIO}\") and flip it from DRIVEN in {FLIPS}",
                switch.name
            ));
        }
    }
    Ok(problems)
}
