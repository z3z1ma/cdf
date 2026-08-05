use std::{env, path::Path, path::PathBuf, process::Command};

fn git(workspace_root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_owned())
}

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .unwrap_or(&manifest_dir);
    let revision = git(workspace_root, &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let dirty = git(
        workspace_root,
        &[
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            "Cargo.toml",
            "Cargo.lock",
            "crates",
        ],
    )
    .is_none_or(|status| !status.is_empty());
    println!("cargo:rustc-env=CDF_BENCHMARK_BUILD_GIT_REVISION={revision}");
    println!("cargo:rustc-env=CDF_BENCHMARK_BUILD_GIT_DIRTY={dirty}");

    if let Some(git_dir) = git(workspace_root, &["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={git_dir}/HEAD");
        if let Some(reference) = git(workspace_root, &["symbolic-ref", "HEAD"]) {
            println!("cargo:rerun-if-changed={git_dir}/{reference}");
        }
    }
}
