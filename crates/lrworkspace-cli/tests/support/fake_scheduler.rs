//! Test-only SSH/scheduler processes. Every command executes on temporary local
//! fixtures; no network connection or real scheduler request is made.
fn main() {
    let program=std::env::current_exe().unwrap().file_stem().unwrap().to_string_lossy().to_ascii_lowercase();
    let args:Vec<_>=std::env::args().skip(1).collect();
    match program.as_str() {
        "head"=>{
            let root=std::path::PathBuf::from(std::env::var_os("LRW_TEST_POLL_ROOT").expect("poll fixture root"));
            let first=std::fs::OpenOptions::new().write(true).create_new(true).open(root.join("first-poll-head")).is_ok();
            if first {
                let deadline=std::time::Instant::now()+std::time::Duration::from_secs(10);
                while !root.join("release-poll-head").exists() {
                    if std::time::Instant::now()>deadline {eprintln!("poll fixture gate timed out");std::process::exit(1);}
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
            println!("poll-fixture");
        }
        "setsid"=>{
            #[cfg(unix)] {
                use std::os::unix::process::CommandExt;
                unsafe extern "C" {fn setsid()->i32;}
                if unsafe {setsid()}<0 {std::process::exit(1);}
                let error=std::process::Command::new(&args[0]).args(&args[1..]).exec();
                eprintln!("fixture exec failed: {error}");std::process::exit(1);
            }
            #[cfg(windows)] {
                use std::os::windows::process::CommandExt;
                // Native CreateProcess searches System32 before PATH; select GNU timeout, not TIMEOUT.EXE.
                let program=std::env::split_paths(&std::env::var_os("PATH").unwrap()).find_map(|directory|{
                    let path=directory.join(format!("{}.exe",args[0].trim_end_matches(".exe")));
                    path.is_file().then_some(path)
                }).unwrap_or_else(||std::path::PathBuf::from(&args[0]));
                let status=std::process::Command::new(program).args(&args[1..])
                    .creation_flags(0x08000200).status().unwrap();
                std::process::exit(status.code().unwrap_or(1));
            }
        }
        "hostname"=>{
            let compute=std::env::var_os("LRW_TEST_ON_COMPUTE").is_some() && std::env::var_os("LRW_TEST_SAME_HOST").is_none();
            let host=if compute {std::env::var("LRW_TEST_TARGET_HOST").unwrap_or_else(|_|"compute01".into())} else {"login-fixture".into()};
            println!("{host}");
        }
        "getent"=>{
            // This executable prevents fixture tests from making real DNS requests.
            if let Ok(name)=std::env::var("LRW_TEST_CANONICAL_HOST") {
                println!("192.0.2.11 STREAM {name}");
                println!("192.0.2.11 DGRAM");
            } else {std::process::exit(2);}
        }
        "ssh"=>{
            if std::env::var_os("LRW_TEST_BLOCK_STDIN").is_some(){std::thread::sleep(std::time::Duration::from_secs(4));return;}
            if args.iter().any(|arg|arg=="-n") {
                use std::io::Write;
                let mut log=std::fs::OpenOptions::new().create(true).append(true).open(std::env::var_os("LRW_TEST_SSH_LOG").unwrap()).unwrap();
                writeln!(log,"{:?}",args).unwrap();
                if std::env::var_os("LRW_TEST_COMPUTE_FAILURE").is_some() {eprintln!("test compute SSH unavailable");std::process::exit(255);}
                let remote=format!("export PATH=\"$LRW_TEST_BIN_POSIX:$PATH\"\n{}",args.last().unwrap().replacen("bash -lc ","bash --noprofile --norc -c ",1));
                let peer=std::env::var("LRW_TEST_SERVER_IP").unwrap_or_else(|_|"192.0.2.11".into());
                let status=std::process::Command::new(std::env::var_os("LRW_TEST_BASH").unwrap()).args(["--noprofile","--norc","-c",&remote]).env("LRW_TEST_ON_COMPUTE","1")
                    .env("SSH_CONNECTION",format!("192.0.2.1 12345 {peer} 22")).status().unwrap();
                std::process::exit(status.code().unwrap_or(1));
            }
            let mut command=std::process::Command::new(std::env::var_os("LRW_TEST_BASH").expect("test bash"));
            command.args(["--noprofile","--norc","-c","export PATH=\"$LRW_TEST_BIN_POSIX:$PATH\"; exec bash --noprofile --norc -s"]);
            #[cfg(windows)] {use std::os::windows::process::CommandExt;command.creation_flags(0x08000000);}
            let status=command.status().unwrap();std::process::exit(status.code().unwrap_or(1));
        }
        "sinfo"=> {
            if std::env::var_os("LRW_TEST_NO_COMPUTE").is_none() {
                if args.iter().any(|arg|arg.contains("%n")) {
                    let state=if std::env::var_os("LRW_TEST_IDLE_COMPUTE").is_some(){"idle"}else{"alloc"};
                    if args.iter().any(|arg|arg=="-N"||arg=="--Node") {println!("compute01 32 128000 (null) {state} default");}
                    else {println!("compute[01-02] 32 128000 (null) idle default");}
                }
                else {println!("default* 1 1-00:00:00 1:00:00 compute01");}
            }
        }
        "qstat"|"sbatch"|"qsub"=>{
            if program=="qstat" || args.iter().any(|arg|arg=="--version"||arg=="--help") {
                use std::io::Write;
                if let Some(path)=std::env::var_os("LRW_TEST_PBS_PROBE_LOG") {
                    writeln!(std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap(),"{program} {}",args.join(" ")).unwrap();
                }
                let mode=std::env::var("LRW_TEST_PBS_VERSION").unwrap_or_else(|_|"openpbs".into());
                match (mode.as_str(),program.as_str(),args.first().map(String::as_str)) {
                    ("openpbs",_,_)=>println!("pbs_version = 23.06.06"),
                    ("torque-bare",_,Some("--version"))=>eprintln!("Version: 6.1.3\nCommit: fixture"),
                    ("fallback-torque","qstat",Some("--version"))=>eprintln!("Version: 6.1.3\nCommit: fixture"),
                    ("help-pro","qsub",Some("--help"))=>{eprintln!("usage: qsub [-j join] [-J range] [-l resource_list] [script]");std::process::exit(2);},
                    ("help-torque","qsub",Some("--help"))=>{eprintln!("usage: qsub [-j join] [-t range] [-l resource_list] [script]");std::process::exit(2);},
                    ("server-pro","qstat",Some("-Bf"))=>println!("Server: fixture\n    pbs_version = 23.06.06"),
                    _=>{eprintln!("site wrapper: version unavailable");std::process::exit(2);},
                }
                return;
            }
            let log=std::env::var_os("LRW_TEST_SUBMIT_LOG").expect("test log");
            use std::io::Write;
            let mut file=std::fs::OpenOptions::new().create(true).append(true).open(log).unwrap();
            writeln!(file,"{} {}",program,args.join(" ")).unwrap();
            if program=="sbatch"{println!("Submitted batch job 4242");}else{println!("4243[].server");}
        }
        "rsync"|"scp"=>{
            if args.iter().any(|arg|arg=="--version") { println!("rsync version 3.4.1 protocol version 32"); return; }
            use std::io::Write;
            let mut log=std::fs::OpenOptions::new().create(true).append(true).open(std::env::var_os("LRW_TEST_TRANSFER_LOG").unwrap()).unwrap();
            writeln!(log,"{program}").unwrap();
            if std::env::var_os(format!("LRW_TEST_FAIL_{}",program.to_uppercase())).is_some() { eprintln!("fixture transport unavailable"); std::process::exit(1); }
            let path=|arg:&str| {
                let value=if arg.contains('@') {arg.split_once(':').unwrap().1} else {arg};
                #[cfg(windows)] let value=if value.starts_with('/') && value.as_bytes().get(2)==Some(&b'/') {format!("{}:{}", &value[1..2], &value[2..])} else {value.to_string()};
                std::path::PathBuf::from(value)
            };
            let source=path(&args[args.len()-2]); let target=path(args.last().unwrap());
            std::fs::copy(source,&target).unwrap();
            if std::env::var_os("LRW_TEST_TRANSFER_TRUNCATED").is_some() { std::fs::write(target,b"bad").unwrap(); }
        }
        _=>std::process::exit(2),
    }
}
