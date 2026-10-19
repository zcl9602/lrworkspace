//! Test-only fake SSH: it forwards rsync's server subprocess locally, never
//! opens a socket and never executes a shell command supplied by a remote host.
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    eprintln!("local rsync peer args: {args:?}");
    if !args.iter().any(|arg| arg == "lrw-local-peer") {
        eprintln!("fake SSH refuses any non-test endpoint"); std::process::exit(90);
    }
    let Some(server) = args.iter().position(|arg| arg == "--server") else {
        eprintln!("fake SSH requires rsync server mode"); std::process::exit(91);
    };
    let executable = std::env::var_os("LRW_TEST_RSYNC").expect("rsync path provided by local test");
    // Preserve rsync's exact pipe/EOF semantics. A relay thread adds another
    // buffering layer to its bidirectional protected-arguments handshake.
    #[cfg(unix)] {
        use std::os::unix::process::CommandExt;
        let error=std::process::Command::new(executable).args(&args[server..]).exec();
        eprintln!("local rsync server failed to start: {error}");
    }
    #[cfg(not(unix))] {
        let _=(executable,server);
        eprintln!("the fake SSH protocol peer is Unix-only; Windows tests local mode");
    }
    std::process::exit(92);
}
