use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_kernel::CdfError;
use clap::Command as ClapCommand;
use clap_complete::{
    generate_to,
    shells::{Bash, Fish, PowerShell, Zsh},
};
use clap_mangen::Man;

use crate::{args, error_catalog, output::CliError};

const COMPLETIONS_DIR: &str = "completions";
const HELP_DIR: &str = "help";
const MAN_DIR: &str = "man";
const COMMAND_DOCS_DIR: &str = "commands";
const ERROR_DOCS_DIR: &str = "errors";

pub fn default_artifact_dir() -> PathBuf {
    workspace_root()
        .join("crates")
        .join("cdf-cli")
        .join("generated")
}

pub fn default_docs_dir() -> PathBuf {
    workspace_root().join("docs")
}

pub fn generate_cli_artifacts(out_dir: &Path) -> Result<(), CliError> {
    reset_child_dir(out_dir, COMPLETIONS_DIR)?;
    reset_child_dir(out_dir, HELP_DIR)?;
    reset_child_dir(out_dir, MAN_DIR)?;

    generate_completions(&out_dir.join(COMPLETIONS_DIR))?;
    generate_help_snapshots(&out_dir.join(HELP_DIR))?;
    generate_man_pages(&out_dir.join(MAN_DIR))?;
    Ok(())
}

pub fn check_cli_artifacts(out_dir: &Path) -> Result<(), CliError> {
    let temp_dir = unique_temp_dir()?;
    let result = (|| {
        generate_cli_artifacts(&temp_dir).map_err(private_artifact_error)?;
        let expected = read_tree(&temp_dir).map_err(private_artifact_error)?;
        let actual = read_tree(out_dir)?;
        compare_trees(&expected, &actual)
    })();
    let _ = fs::remove_dir_all(&temp_dir);
    result
}

pub fn generate_reference_docs(docs_dir: &Path) -> Result<(), CliError> {
    reset_child_dir(docs_dir, COMMAND_DOCS_DIR)?;
    reset_child_dir(docs_dir, ERROR_DOCS_DIR)?;
    generate_command_docs(&docs_dir.join(COMMAND_DOCS_DIR))?;
    generate_error_docs(&docs_dir.join(ERROR_DOCS_DIR))
}

pub fn check_reference_docs(docs_dir: &Path) -> Result<(), CliError> {
    let temp_dir = unique_temp_dir()?;
    let result = (|| {
        generate_reference_docs(&temp_dir).map_err(private_artifact_error)?;
        let expected = read_tree(&temp_dir).map_err(private_artifact_error)?;
        let actual = read_reference_tree(docs_dir)?;
        compare_reference_trees(&expected, &actual, docs_dir)
    })();
    let _ = fs::remove_dir_all(&temp_dir);
    result
}

fn generate_command_docs(out_dir: &Path) -> Result<(), CliError> {
    fs::create_dir_all(out_dir).map_err(artifact_io_error("create command docs directory"))?;
    let paths = command_paths(&args::cli_command());
    let mut index = String::from(
        "# Command reference\n\nGenerated from the CLI's clap definitions. Do not edit these pages by hand.\n\n",
    );
    for path in &paths {
        let file_name = artifact_file_name(path, "md");
        let title = bin_name(path);
        index.push_str(&format!("- [`{title}`]({file_name})\n"));
        let help = normalize_generated_text(&args::render_help(path)?);
        let page = format!(
            "# `{title}`\n\nGenerated from the CLI's clap definitions.\n\n```text\n{help}```\n"
        );
        fs::write(out_dir.join(file_name), page).map_err(artifact_io_error("write command doc"))?;
    }
    fs::write(out_dir.join("README.md"), index)
        .map_err(artifact_io_error("write command docs index"))
}

fn generate_error_docs(out_dir: &Path) -> Result<(), CliError> {
    fs::create_dir_all(out_dir).map_err(artifact_io_error("create error docs directory"))?;
    let mut entries = error_catalog::reference_entries();
    entries.sort_by_key(|(_, mapping)| mapping.code);
    let mut page = String::from(
        "# Error reference\n\nGenerated from the CLI error catalog. Do not edit this page by hand.\n\n| Code | Area | Kind | Exit | Meaning | Remediation | Representative command |\n|---|---|---|---:|---|---|---|\n",
    );
    for (_, mapping) in entries {
        let remediation = mapping.remediation.map_or_else(
            || "No remediation is registered.".to_owned(),
            |remediation| {
                let mut text = remediation.summary.to_owned();
                for step in remediation.steps {
                    text.push(' ');
                    text.push_str(step);
                }
                text
            },
        );
        page.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | `{}` |\n",
            mapping.code,
            error_area(mapping.code),
            error_kind(mapping.code, mapping.exit_code),
            mapping.exit_code,
            error_meaning(mapping.code),
            markdown_cell(&remediation),
            representative_command(mapping.code),
        ));
    }
    fs::write(out_dir.join("README.md"), page).map_err(artifact_io_error("write error reference"))
}

fn error_area(code: &str) -> &str {
    code.split('-').nth(1).unwrap_or("INTERNAL")
}

fn error_kind(code: &str, exit_code: i32) -> &'static str {
    if error_area(code) == "ENV" {
        return "environment";
    }
    match exit_code {
        4 => "auth",
        5 => "data",
        6 => "destination",
        70 => "internal",
        75 => "transient",
        2 | 3 | 78 => "contract",
        _ => "internal",
    }
}

fn error_meaning(code: &str) -> String {
    code.split('-')
        .skip(2)
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn representative_command(code: &str) -> &'static str {
    match error_area(code) {
        "CONTRACT" => "cdf contract show",
        "DEST" => "cdf plan",
        "DOCTOR" => "cdf doctor",
        "ENV" => "cdf doctor",
        "PACKAGE" => "cdf package verify",
        "PROJECT" => "cdf validate",
        "RESOURCE" => "cdf inspect resources",
        "RUN" => "cdf run",
        "SQL" => "cdf sql",
        "STATE" => "cdf state show",
        "STATUS" => "cdf status",
        _ => "cdf help",
    }
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

fn generate_completions(out_dir: &Path) -> Result<(), CliError> {
    fs::create_dir_all(out_dir).map_err(artifact_io_error("create completion directory"))?;
    for shell in [
        CompletionShell::Bash,
        CompletionShell::Zsh,
        CompletionShell::Fish,
        CompletionShell::PowerShell,
    ] {
        let mut command = args::cli_command();
        match shell {
            CompletionShell::Bash => generate_to(Bash, &mut command, "cdf", out_dir),
            CompletionShell::Zsh => generate_to(Zsh, &mut command, "cdf", out_dir),
            CompletionShell::Fish => generate_to(Fish, &mut command, "cdf", out_dir),
            CompletionShell::PowerShell => generate_to(PowerShell, &mut command, "cdf", out_dir),
        }
        .map_err(artifact_io_error("generate shell completion"))?;
    }
    Ok(())
}

fn generate_help_snapshots(out_dir: &Path) -> Result<(), CliError> {
    fs::create_dir_all(out_dir).map_err(artifact_io_error("create help snapshot directory"))?;
    for path in command_paths(&args::cli_command()) {
        let text = normalize_generated_text(&args::render_help(&path)?);
        fs::write(out_dir.join(artifact_file_name(&path, "txt")), text)
            .map_err(artifact_io_error("write help snapshot"))?;
    }
    Ok(())
}

fn generate_man_pages(out_dir: &Path) -> Result<(), CliError> {
    fs::create_dir_all(out_dir).map_err(artifact_io_error("create man page directory"))?;
    let mut root = args::cli_command();
    root.build();
    for path in command_paths(&root) {
        let mut command = command_at_path(&root, &path).ok_or_else(|| {
            internal(format!(
                "missing command path for man page: {}",
                path.join(" ")
            ))
        })?;
        command = command.name(artifact_stem(&path)).bin_name(bin_name(&path));
        let mut page = Vec::new();
        Man::new(command)
            .render(&mut page)
            .map_err(|error| generator_io_error("render man page", error))?;
        let page = String::from_utf8(page)
            .map_err(|_| internal("generated man page must be valid UTF-8"))?;
        fs::write(
            out_dir.join(artifact_file_name(&path, "1")),
            normalize_generated_text(&page),
        )
        .map_err(artifact_io_error("write man page"))?;
    }
    Ok(())
}

fn command_paths(root: &ClapCommand) -> Vec<Vec<String>> {
    let mut paths = Vec::new();
    let mut current = Vec::new();
    collect_command_paths(root, &mut current, &mut paths);
    paths
}

fn collect_command_paths(
    command: &ClapCommand,
    current: &mut Vec<String>,
    paths: &mut Vec<Vec<String>>,
) {
    paths.push(current.clone());
    for subcommand in command
        .get_subcommands()
        .filter(|subcommand| !subcommand.is_hide_set())
    {
        current.push(subcommand.get_name().to_owned());
        collect_command_paths(subcommand, current, paths);
        current.pop();
    }
}

fn command_at_path(root: &ClapCommand, path: &[String]) -> Option<ClapCommand> {
    let mut command = root.clone();
    for name in path {
        let subcommand = command
            .get_subcommands()
            .find(|subcommand| subcommand.get_name() == name)?
            .clone();
        command = subcommand;
    }
    Some(command)
}

fn artifact_file_name(path: &[String], extension: &str) -> String {
    format!("{}.{}", artifact_stem(path), extension)
}

fn artifact_stem(path: &[String]) -> String {
    if path.is_empty() {
        "cdf".to_owned()
    } else {
        format!("cdf-{}", path.join("-"))
    }
}

fn bin_name(path: &[String]) -> String {
    if path.is_empty() {
        "cdf".to_owned()
    } else {
        format!("cdf {}", path.join(" "))
    }
}

fn normalize_generated_text(text: &str) -> String {
    let mut lines = text
        .lines()
        .map(|line| line.trim_end_matches([' ', '\t']))
        .collect::<Vec<_>>();
    while matches!(lines.last(), Some(line) if line.is_empty()) {
        lines.pop();
    }
    let mut normalized = lines.join("\n");
    normalized.push('\n');
    normalized
}

fn reset_child_dir(root: &Path, child: &str) -> Result<(), CliError> {
    let path = root.join(child);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.is_dir() => {
            fs::remove_dir_all(&path)
                .map_err(artifact_io_error("remove generated artifact directory"))?;
        }
        Ok(_) => {
            fs::remove_file(&path).map_err(artifact_io_error("remove generated artifact entry"))?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(artifact_io_error("inspect generated artifact directory")(
                error,
            ));
        }
    }
    fs::create_dir_all(path).map_err(artifact_io_error("create generated artifact directory"))
}

fn read_tree(root: &Path) -> Result<BTreeMap<PathBuf, Vec<u8>>, CliError> {
    let mut files = BTreeMap::new();
    read_tree_inner(root, root, &mut files)?;
    Ok(files)
}

fn read_reference_tree(root: &Path) -> Result<BTreeMap<PathBuf, Vec<u8>>, CliError> {
    let mut files = BTreeMap::new();
    for child in [COMMAND_DOCS_DIR, ERROR_DOCS_DIR] {
        read_tree_inner(root, &root.join(child), &mut files)?;
    }
    Ok(files)
}

fn read_tree_inner(
    root: &Path,
    path: &Path,
    files: &mut BTreeMap<PathBuf, Vec<u8>>,
) -> Result<(), CliError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(missing_artifact_directory(path));
        }
        Err(error) => return Err(artifact_read_error("inspect", path, error)),
    };
    if !metadata.is_dir() {
        return Err(artifact_shape_error(path, "expected a directory"));
    }
    for entry in
        fs::read_dir(path).map_err(|error| artifact_read_error("read directory", path, error))?
    {
        let entry =
            entry.map_err(|error| artifact_read_error("read directory entry", path, error))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| artifact_read_error("inspect entry", &path, error))?;
        if file_type.is_dir() {
            read_tree_inner(root, &path, files)?;
        } else if file_type.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| internal(format!("strip artifact prefix: {error}")))?;
            files.insert(
                relative.to_path_buf(),
                fs::read(&path).map_err(|error| artifact_read_error("read file", &path, error))?,
            );
        } else {
            return Err(artifact_shape_error(
                &path,
                "symlinks and special filesystem entries are unsupported",
            ));
        }
    }
    Ok(())
}

fn missing_artifact_directory(path: &Path) -> CliError {
    CliError::usage_with(
        format!(
            "generated CLI artifact directory does not exist: {}",
            path.display()
        ),
        error_catalog::CLI_ARTIFACTS_USAGE,
    )
}

fn artifact_read_error(action: &str, path: &Path, error: io::Error) -> CliError {
    if matches!(
        error.kind(),
        io::ErrorKind::NotFound
            | io::ErrorKind::NotADirectory
            | io::ErrorKind::IsADirectory
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::InvalidData
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        artifact_shape_error(path, &format!("{action} failed: {error}"))
    } else {
        environment(format!(
            "{action} committed CLI artifact {}: {error}; check filesystem permissions, device availability, memory, and process file limits before retrying",
            path.display()
        ))
    }
}

fn artifact_shape_error(path: &Path, detail: &str) -> CliError {
    CdfError::data(format!(
        "committed CLI artifact {} has an unsupported filesystem shape: {detail}",
        path.display()
    ))
    .into()
}

fn compare_trees(
    expected: &BTreeMap<PathBuf, Vec<u8>>,
    actual: &BTreeMap<PathBuf, Vec<u8>>,
) -> Result<(), CliError> {
    let mut drift = Vec::new();
    for path in expected.keys() {
        match actual.get(path) {
            Some(bytes) if bytes == &expected[path] => {}
            Some(_) => drift.push(format!("stale {}", path.display())),
            None => drift.push(format!("missing {}", path.display())),
        }
    }
    for path in actual.keys() {
        if !expected.contains_key(path) {
            drift.push(format!("extra {}", path.display()));
        }
    }
    if drift.is_empty() {
        Ok(())
    } else {
        Err(CliError::usage_with(
            format!(
                "generated CLI artifacts are stale; run `cargo run -p cdf-cli-core --locked --features cli-artifacts --bin cdf-generate-cli-artifacts -- --out-dir {}`:\n{}",
                default_artifact_dir().display(),
                drift.join("\n")
            ),
            error_catalog::CLI_ARTIFACTS_USAGE,
        ))
    }
}

fn compare_reference_trees(
    expected: &BTreeMap<PathBuf, Vec<u8>>,
    actual: &BTreeMap<PathBuf, Vec<u8>>,
    docs_dir: &Path,
) -> Result<(), CliError> {
    let mut drift = Vec::new();
    for path in expected.keys() {
        match actual.get(path) {
            Some(bytes) if bytes == &expected[path] => {}
            Some(_) => drift.push(format!("stale {}", path.display())),
            None => drift.push(format!("missing {}", path.display())),
        }
    }
    for path in actual.keys() {
        if !expected.contains_key(path) {
            drift.push(format!("extra {}", path.display()));
        }
    }
    if drift.is_empty() {
        Ok(())
    } else {
        Err(CliError::usage_with(
            format!(
                "generated command and error reference is stale; run `cargo run -p cdf-cli-core --locked --features cli-artifacts --bin cdf-generate-cli-artifacts -- --docs-dir {} --docs-only`:\n{}",
                docs_dir.display(),
                drift.join("\n")
            ),
            error_catalog::CLI_ARTIFACTS_USAGE,
        ))
    }
}

fn unique_temp_dir() -> Result<PathBuf, CliError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            environment(format!(
                "read the host clock for a temporary CLI artifact directory: {error}; correct the system clock and retry"
            ))
        })?
        .as_nanos();
    let root = workspace_root().join("target").join("quality").join("tmp");
    fs::create_dir_all(&root).map_err(private_artifact_io_error(
        "create temporary artifact parent directory",
    ))?;
    for attempt in 0..100 {
        let path = root.join(format!(
            "cdf-cli-artifacts-{}-{nanos}-{attempt}",
            std::process::id()
        ));
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(private_artifact_io_error(
                    "create temporary CLI artifact directory",
                )(error));
            }
        }
    }
    Err(internal(
        "create temporary CLI artifact directory: exhausted 100 unique CDF-managed path attempts",
    ))
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

fn io_error(context: &'static str) -> impl Fn(io::Error) -> CliError {
    move |error| {
        environment(format!(
            "{context}: {error}; check filesystem permissions, free space, device availability, and process file limits before retrying"
        ))
    }
}

fn artifact_io_error(context: &'static str) -> impl Fn(io::Error) -> CliError {
    move |error| {
        if matches!(
            error.kind(),
            io::ErrorKind::NotFound
                | io::ErrorKind::NotADirectory
                | io::ErrorKind::IsADirectory
                | io::ErrorKind::AlreadyExists
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::InvalidInput
                | io::ErrorKind::InvalidData
        ) || cdf_kernel::is_filesystem_loop(&error)
        {
            CliError::mapped(
                CdfError::data(format!(
                    "{context}: generated artifact path has an invalid filesystem shape: {error}"
                )),
                error_catalog::CLI_ARTIFACTS,
            )
        } else {
            io_error(context)(error)
        }
    }
}

fn generator_io_error(context: &str, error: io::Error) -> CliError {
    internal(format!("{context}: in-memory generator failed: {error}"))
}

fn private_artifact_io_error(context: &'static str) -> impl Fn(io::Error) -> CliError {
    move |error| {
        if matches!(
            error.kind(),
            io::ErrorKind::NotFound
                | io::ErrorKind::NotADirectory
                | io::ErrorKind::IsADirectory
                | io::ErrorKind::AlreadyExists
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::InvalidInput
                | io::ErrorKind::InvalidData
        ) || cdf_kernel::is_filesystem_loop(&error)
        {
            internal(format!(
                "{context}: CDF-managed artifact scratch has an invalid filesystem shape: {error}"
            ))
        } else {
            io_error(context)(error)
        }
    }
}

fn private_artifact_error(error: CliError) -> CliError {
    if error.kind == cdf_kernel::ErrorKind::Data {
        internal(format!(
            "generate CDF-managed expected artifact tree: {}",
            error.message
        ))
    } else {
        error
    }
}

fn environment(message: impl Into<String>) -> CliError {
    CdfError::environment(message.into()).into()
}

fn internal(message: impl Into<String>) -> CliError {
    CliError::mapped(
        CdfError::internal(message.into()),
        error_catalog::CLI_ARTIFACTS,
    )
}

enum CompletionShell {
    Bash,
    Zsh,
    Fish,
    PowerShell,
}

#[cfg(test)]
mod tests {
    use std::fs;

    use cdf_kernel::ErrorKind;

    use super::{
        check_cli_artifacts, default_artifact_dir, environment, error_kind, generate_cli_artifacts,
        generator_io_error, io_error, read_tree, unique_temp_dir,
    };

    #[test]
    fn environment_reference_kind_is_not_inferred_from_shared_exit_code() {
        assert_eq!(error_kind("CDF-ENV-HOST", 70), "environment");
        assert_eq!(error_kind("CDF-INTERNAL-UNEXPECTED", 70), "internal");
    }

    #[test]
    fn artifact_host_failures_use_environment_mapping_and_remediation() {
        let direct = environment("temporary artifact host failure");
        assert_eq!(direct.kind, ErrorKind::Environment);
        assert_eq!(direct.code, "CDF-ENV-HOST");
        assert!(direct.remediation.is_some());

        let io = io_error("write generated artifact")(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        ));
        assert_eq!(io.kind, ErrorKind::Environment);
        assert!(io.message.contains("filesystem permissions"));
    }

    #[test]
    fn public_artifact_generation_rejects_wrong_root_shape_as_data() {
        let root = unique_temp_dir().unwrap();
        let output = root.join("output");
        fs::write(&output, b"not a directory").unwrap();

        let error = generate_cli_artifacts(&output).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Data);
        assert!(error.message.contains("invalid filesystem shape"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn in_memory_generator_io_is_internal() {
        let error = generator_io_error(
            "render man page",
            std::io::Error::from(std::io::ErrorKind::Other),
        );

        assert_eq!(error.kind, ErrorKind::Internal);
        assert!(error.message.contains("in-memory generator"));
    }

    #[test]
    fn cli_generated_artifacts_match_committed_snapshots() {
        check_cli_artifacts(&default_artifact_dir()).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn committed_artifact_traversal_rejects_symlinks_instead_of_skipping_them() {
        use std::os::unix::fs::symlink;

        let root = unique_temp_dir().unwrap();
        fs::write(root.join("target.txt"), b"target").unwrap();
        symlink("target.txt", root.join("alias.txt")).unwrap();

        let error = read_tree(&root).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Data);
        assert!(error.message.contains("symlinks"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn committed_artifact_traversal_reports_parent_file_shape_as_data() {
        let root = unique_temp_dir().unwrap();
        let parent = root.join("parent");
        fs::write(&parent, b"not a directory").unwrap();

        let error = read_tree(&parent.join("child")).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Data);
        assert!(error.message.contains("unsupported filesystem shape"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cx1_generated_help_and_man_pages_are_complete_and_share_global_authority() {
        let generated = default_artifact_dir();
        for child in ["help", "man"] {
            for entry in fs::read_dir(generated.join(child)).unwrap() {
                let text = fs::read_to_string(entry.unwrap().path()).unwrap();
                assert!(!text.contains("Command option"));
                assert!(!text.contains("Command value"));
            }
        }

        let root = fs::read_to_string(generated.join("help/cdf.txt")).unwrap();
        assert!(root.contains("Environment:"));
        assert!(root.contains("Examples:"));
        let run_man = fs::read_to_string(generated.join("man/cdf-run.1")).unwrap();
        for global in ["\\-\\-color", "\\-\\-progress", "\\-\\-unicode"] {
            assert!(run_man.contains(global), "run man page missing {global}");
        }
        for description in [
            "Color policy: auto, always, or never",
            "Progress policy: auto, always, or never",
            "Unicode policy: auto, always, or never",
        ] {
            assert!(
                run_man.contains(description),
                "run man page missing {description}"
            );
        }
        let bash = fs::read_to_string(generated.join("completions/cdf.bash")).unwrap();
        assert!(bash.matches("auto always never").count() >= 3);
    }
}
