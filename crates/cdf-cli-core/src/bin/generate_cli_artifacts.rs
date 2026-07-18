use std::path::PathBuf;

use cdf_cli_core::cli_artifacts::{
    check_cli_artifacts, check_reference_docs, default_artifact_dir, default_docs_dir,
    generate_cli_artifacts, generate_reference_docs,
};

fn main() {
    // nosemgrep: rust.lang.security.args.args -- argv is parsed for local artifact-generation flags only; argv[0] is ignored.
    match run(std::env::args().skip(1).collect()) {
        Ok(message) => {
            println!("{message}");
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn run(args: Vec<String>) -> Result<String, String> {
    let mut out_dir = default_artifact_dir();
    let mut docs_dir = default_docs_dir();
    let mut check = false;
    let mut docs_only = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--out-dir" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "--out-dir requires a value".to_owned())?;
                out_dir = PathBuf::from(value);
                index += 2;
            }
            "--check" => {
                check = true;
                index += 1;
            }
            "--docs-dir" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "--docs-dir requires a value".to_owned())?;
                docs_dir = PathBuf::from(value);
                index += 2;
            }
            "--docs-only" => {
                docs_only = true;
                index += 1;
            }
            "-h" | "--help" => return Ok(usage()),
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    if docs_only && check {
        check_reference_docs(&docs_dir).map_err(|error| error.message)?;
        Ok(format!(
            "generated command and error reference is fresh in {}",
            docs_dir.display()
        ))
    } else if docs_only {
        generate_reference_docs(&docs_dir).map_err(|error| error.message)?;
        Ok(format!(
            "generated command and error reference in {}",
            docs_dir.display()
        ))
    } else if check {
        check_cli_artifacts(&out_dir).map_err(|error| error.message)?;
        Ok(format!(
            "generated CLI artifacts are fresh in {}",
            out_dir.display()
        ))
    } else {
        generate_cli_artifacts(&out_dir).map_err(|error| error.message)?;
        Ok(format!("generated CLI artifacts in {}", out_dir.display()))
    }
}

fn usage() -> String {
    "Usage: cdf-generate-cli-artifacts [--out-dir DIR] [--docs-dir DIR --docs-only] [--check]"
        .to_owned()
}
