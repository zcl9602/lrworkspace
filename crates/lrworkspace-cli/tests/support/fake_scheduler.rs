//! Test-only SSH/scheduler processes. Every command executes on temporary local
//! fixtures; no network connection or real scheduler request is made.
fn main() {
    let program=std::env::current_exe().unwrap().file_stem().unwrap().to_string_lossy().to_ascii_lowercase();
    let args:Vec<_>=std::env::args().skip(1).collect();
    match program.as_str() {
        "hostname"=>{
            let compute=std::env::var_os("LRW_TEST_ON_COMPUTE").is_some() && std::env::var_os("LRW_TEST_SAME_HOST").is_none();
            println!("{}",if compute {"compute-fixture"} else {"login-fixture"});
        }
        "ssh"=>{
            if std::env::var_os("LRW_TEST_BLOCK_STDIN").is_some(){std::thread::sleep(std::time::Duration::from_secs(4));return;}
            if args.iter().any(|arg|arg=="-n") {
                use std::io::Write;
                let mut log=std::fs::OpenOptions::new().create(true).append(true).open(std::env::var_os("LRW_TEST_SSH_LOG").unwrap()).unwrap();
                writeln!(log,"{:?}",args).unwrap();
                if std::env::var_os("LRW_TEST_COMPUTE_FAILURE").is_some() {eprintln!("test compute SSH unavailable");std::process::exit(255);}
                let remote=format!("export PATH=\"$LRW_TEST_BIN_POSIX:$PATH\"\n{}",args.last().unwrap().replacen("bash -lc ","bash --noprofile --norc -c ",1));
                let status=std::process::Command::new(std::env::var_os("LRW_TEST_BASH").unwrap()).args(["--noprofile","--norc","-c",&remote]).env("LRW_TEST_ON_COMPUTE","1").status().unwrap();
                std::process::exit(status.code().unwrap_or(1));
            }
            let mut command=std::process::Command::new(std::env::var_os("LRW_TEST_BASH").expect("test bash"));
            command.args(["--noprofile","--norc","-c","export PATH=\"$LRW_TEST_BIN_POSIX:$PATH\"; exec bash --noprofile --norc -s"]);
            #[cfg(windows)] {use std::os::windows::process::CommandExt;command.creation_flags(0x08000000);}
            let status=command.status().unwrap();std::process::exit(status.code().unwrap_or(1));
        }
        "sbatch"|"qsub"=>{
            if args.iter().any(|arg|arg=="--version") {println!("pbs_version = 23.06.06");return;}
            let log=std::env::var_os("LRW_TEST_SUBMIT_LOG").expect("test log");
            use std::io::Write;
            let mut file=std::fs::OpenOptions::new().create(true).append(true).open(log).unwrap();
            writeln!(file,"{} {}",program,args.join(" ")).unwrap();
            if program=="sbatch"{println!("Submitted batch job 4242");}else{println!("4243[].server");}
        }
        "rsync"|"scp"=>{eprintln!("test binary transport disabled; use base64 through fake SSH");std::process::exit(1);}
        _=>std::process::exit(2),
    }
}
