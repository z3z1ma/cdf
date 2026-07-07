fn main() {
    // nosemgrep: rust.lang.security.args.args -- argv is parsed for CLI dispatch only; argv[0] is not trusted for security decisions.
    let result = cdf_cli::invoke(std::env::args().map(std::ffi::OsString::from));
    print!("{}", result.stdout);
    eprint!("{}", result.stderr);
    std::process::exit(result.exit_code);
}
