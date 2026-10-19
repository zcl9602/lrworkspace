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
    use std::io::{Read, Write};
    use std::process::Stdio;
    let mut child = std::process::Command::new(executable).args(&args[server..])
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
    let mut input = child.stdin.take().unwrap();
    std::thread::spawn(move || { let _ = std::io::copy(&mut std::io::stdin().lock(), &mut input); });
    let mut output = child.stdout.take().unwrap();
    let mut stdout = std::io::stdout().lock();
    let mut buffer = [0u8; 8192];
    loop {
        let count = output.read(&mut buffer).unwrap();
        if count == 0 { break; }
        stdout.write_all(&buffer[..count]).unwrap();
        stdout.flush().unwrap();
    }
    let status = child.wait().unwrap();
    std::process::exit(status.code().unwrap_or(92));
}
