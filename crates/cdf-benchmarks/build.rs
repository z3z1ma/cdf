use std::{env, path::PathBuf, process::Command};

fn git(manifest_dir: &PathBuf, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(manifest_dir)
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
    let revision = git(&manifest_dir, &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let dirty = git(
        &manifest_dir,
        &["status", "--porcelain", "--untracked-files=no"],
    )
    .is_none_or(|status| !status.is_empty());
    println!("cargo:rustc-env=CDF_BENCHMARK_BUILD_GIT_REVISION={revision}");
    println!("cargo:rustc-env=CDF_BENCHMARK_BUILD_GIT_DIRTY={dirty}");

    if let Some(git_dir) = git(&manifest_dir, &["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={git_dir}/HEAD");
        if let Some(reference) = git(&manifest_dir, &["symbolic-ref", "HEAD"]) {
            println!("cargo:rerun-if-changed={git_dir}/{reference}");
        }
    }
}
