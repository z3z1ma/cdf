use std::path::PathBuf;

use cdf_cli::cli_artifacts::{check_cli_artifacts, default_artifact_dir, generate_cli_artifacts};

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
    let mut check = false;
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
            "-h" | "--help" => return Ok(usage()),
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    if check {
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
    "Usage: cdf-generate-cli-artifacts [--out-dir DIR] [--check]".to_owned()
}
