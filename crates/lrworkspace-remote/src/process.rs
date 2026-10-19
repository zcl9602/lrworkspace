//! Captured subprocess I/O shares one deadline, including stdin and pipe EOF.
use std::process::{Command, Output, Stdio};
use std::time::Duration;
#[cfg(any(test, not(windows)))]
use std::time::Instant;
use lrworkspace_core::{CoreError, CoreResult};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use crate::stream::OutputBuffer;
const CAPTURE_LIMIT: usize = 64 * 1024;

fn copy_command(source: &Command,isolate:bool) -> Command {
    let mut command=Command::new(source.get_program());
    command.args(source.get_args());
    if let Some(path)=source.get_current_dir() {command.current_dir(path);}
    for (name,value) in source.get_envs() {if let Some(value)=value {command.env(name,value);} else {command.env_remove(name);}}
    #[cfg(unix)] {
        use std::os::unix::process::CommandExt;if isolate {command.process_group(0);}
        #[cfg(target_os="linux")]
        // SAFETY: prctl is called in the child before exec; no Rust locks or allocations.
        unsafe {command.pre_exec(||{libc::prctl(libc::PR_SET_PDEATHSIG,libc::SIGTERM);Ok(())});}
    }
    #[cfg(windows)] {let _=isolate;use std::os::windows::process::CommandExt; command.creation_flags(0x08000000);}
    command
}

async fn drain(mut stream:impl AsyncRead+Unpin,retained:&mut OutputBuffer)->std::io::Result<()> {
    let mut chunk=[0u8;8192];
    loop {let count=stream.read(&mut chunk).await?;if count==0 {break;}retained.push(&chunk[..count]);}
    Ok(())
}

async fn stop(child: &mut tokio::process::Child, pid: Option<u32>) {
    #[cfg(unix)] if let Some(pid)=pid {unsafe {libc::kill(-(pid as i32),libc::SIGKILL);}}
    #[cfg(windows)] if let Some(pid)=pid {
        let mut killer=tokio::process::Command::new("taskkill");
        killer.args(["/PID",&pid.to_string(),"/T","/F"]).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).kill_on_drop(true);
        killer.creation_flags(0x08000000);
        let _=tokio::time::timeout(Duration::from_secs(1),killer.status()).await;
    }
    let _=child.start_kill();
    let _=tokio::time::timeout(Duration::from_millis(200),child.wait()).await;
}

/// Used by Unix SSH too; retention limits never stop draining the pipes.
pub(crate) fn capture_input(command:&mut Command, input:&[u8], timeout:Duration, limit:usize, label:&str) -> CoreResult<(Output,bool)> {
    let deadline=std::time::Instant::now()+timeout;
    let command=copy_command(command,true);
    // A short-lived current-thread runtime also works when called by a Tokio
    // blocking worker. No pipe-reader thread can outlive a cancelled operation.
    std::thread::scope(|scope|scope.spawn(move || {
        let runtime=tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let deadline=tokio::time::Instant::from_std(deadline);
            let mut command=tokio::process::Command::from(command);
            command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
            let mut child=command.spawn().map_err(|e|CoreError::Connection(format!("{label} failed to start: {e}")))?;
            let pid=child.id();
            #[cfg(windows)]
            let _tree=child.raw_handle().and_then(|handle|WindowsJob::attach(handle).map_err(|e|tracing::warn!(error=%e,"Job-object cleanup unavailable; using bounded taskkill fallback")).ok());
            let mut stdin=child.stdin.take().expect("piped stdin");
            let stdout=child.stdout.take().expect("piped stdout");
            let stderr=child.stderr.take().expect("piped stderr");
            let mut out_buffer=OutputBuffer::new(limit);let mut err_buffer=OutputBuffer::new(limit);
            let result=tokio::time::timeout_at(deadline, async {
                tokio::try_join!(async {stdin.write_all(input).await?;stdin.shutdown().await},drain(stdout,&mut out_buffer),drain(stderr,&mut err_buffer),child.wait())
            }).await;
            let (stdout,out_cut)=out_buffer.into_bytes();let (stderr,err_cut)=err_buffer.into_bytes();
            match result {
                Ok(Ok((_,_,_,status))) => Ok((Output {status,stdout,stderr},out_cut||err_cut)),
                Ok(Err(error)) => {stop(&mut child,pid).await;Err(CoreError::Connection(format!("{label} I/O failed: {error}; {}",String::from_utf8_lossy(&stderr[..stderr.len().min(4096)]).trim())))},
                Err(_) => {stop(&mut child,pid).await;Err(CoreError::Timeout(format!("{label} exceeded {} seconds (including pipe I/O); {}",timeout.as_secs_f64(),String::from_utf8_lossy(&stderr[..stderr.len().min(4096)]).trim())))},
            }
        })
    }).join().map_err(|_|CoreError::Connection(format!("{label} worker panicked")))?)
}

pub(crate) fn capture(command:&mut Command, timeout:Duration, label:&str)->CoreResult<Output> {
    capture_input(command,&[],timeout,CAPTURE_LIMIT,label).map(|(output,_)|output)
}

#[cfg(not(windows))]
pub(crate) fn inherited_status(command:&mut Command,timeout:Duration,label:&str)->CoreResult<std::process::ExitStatus> {
    let mut command=copy_command(command,false);command.stdin(Stdio::inherit()).stdout(Stdio::inherit()).stderr(Stdio::inherit());
    let mut child=command.spawn()?;let started=Instant::now();
    loop {
        if let Some(status)=child.try_wait()? {return Ok(status);}
        if started.elapsed()>=timeout {let _=child.kill();let _=child.wait();return Err(CoreError::Timeout(format!("{label} exceeded its deadline")));}
        std::thread::sleep(Duration::from_millis(10));
    }
}

pub(crate) fn checked(command: &mut Command, timeout: Duration, label: &str) -> CoreResult<Output> {
    let output = capture(command, timeout, label)?;
    if output.status.success() { return Ok(output); }
    Err(CoreError::Connection(format!("{label} failed (exit {:?}): {} {}",
        output.status.code(), String::from_utf8_lossy(&output.stderr).trim(),
        String::from_utf8_lossy(&output.stdout).trim())))
}

#[cfg(windows)]
struct WindowsJob(windows_sys::Win32::Foundation::HANDLE);
#[cfg(windows)]
impl WindowsJob {
    fn attach(process:std::os::windows::io::RawHandle)->std::io::Result<Self> {
        use windows_sys::Win32::System::JobObjects::*;
        // SAFETY: the owned job handle is closed exactly once; structures use
        // the Windows SDK layout and the process handle is borrowed from Child.
        unsafe {
            let handle=CreateJobObjectW(std::ptr::null(),std::ptr::null());
            if handle.is_null(){return Err(std::io::Error::last_os_error());}
            let job=Self(handle);
            let mut info:JOBOBJECT_EXTENDED_LIMIT_INFORMATION=std::mem::zeroed();
            info.BasicLimitInformation.LimitFlags=JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if SetInformationJobObject(handle,JobObjectExtendedLimitInformation,&info as *const _ as *const _,std::mem::size_of_val(&info) as u32)==0 || AssignProcessToJobObject(handle,process)==0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(job)
        }
    }
}
#[cfg(windows)]
impl Drop for WindowsJob {fn drop(&mut self){unsafe {windows_sys::Win32::Foundation::CloseHandle(self.0);}}}

#[cfg(test)]
mod tests {
    use super::*;
    fn shell(script: &str) -> Command {
        #[cfg(windows)]
        { let mut cmd = Command::new("cmd"); cmd.args(["/C", script]); cmd }
        #[cfg(unix)]
        { let mut cmd = Command::new("sh"); cmd.args(["-c", script]); cmd }
    }

    fn bash(script:&str)->Option<Command> {
        #[cfg(windows)] let path=r"C:\Program Files\Git\bin\bash.exe";
        #[cfg(unix)] let path="bash";
        if !Command::new(path).args(["-c","exit 0"]).status().is_ok_and(|s|s.success()){return None;}
        let mut cmd=Command::new(path);cmd.args(["--noprofile","--norc","-c",script]);Some(cmd)
    }
    #[test]
    fn inherited_pipe_handles_cannot_outlive_the_deadline() {
        let Some(mut command)=bash("sleep 4 & exit 0") else{return};let start=Instant::now();
        assert!(matches!(capture(&mut command,Duration::from_millis(300),"pipe holder"),Err(CoreError::Timeout(_))));
        assert!(start.elapsed()<Duration::from_secs(3));
    }
    #[test]
    fn blocked_stdin_times_out_and_both_streams_are_drained_after_limits() {
        let Some(mut blocked)=bash("sleep 4") else{return};let start=Instant::now();
        assert!(matches!(capture_input(&mut blocked,&vec![b'x';512*1024],Duration::from_millis(300),1024,"blocked stdin"),Err(CoreError::Timeout(_))));
        assert!(start.elapsed()<Duration::from_secs(3));
        let Some(mut noisy)=bash("head -c 131072 /dev/zero; head -c 131072 /dev/zero >&2; exit 7") else{return};
        let (output,truncated)=capture_input(&mut noisy,&[],Duration::from_secs(10),1024,"noisy process").unwrap();
        assert_eq!(output.status.code(),Some(7));assert_eq!(output.stdout.len(),1024);assert_eq!(output.stderr.len(),1024);assert!(truncated);
    }

    #[test]
    fn subprocess_output_is_captured_and_failure_is_diagnostic() {
        let output = checked(&mut shell("echo transfer-output"), Duration::from_secs(5), "test").unwrap();
        assert!(String::from_utf8_lossy(&output.stdout).contains("transfer-output"));
        #[cfg(windows)] let script = "echo transfer-failure 1>&2 & exit /B 7";
        #[cfg(unix)] let script = "echo transfer-failure >&2; exit 7";
        let error = checked(&mut shell(script), Duration::from_secs(5), "test").unwrap_err().to_string();
        assert!(error.contains("7") && error.contains("transfer-failure"), "{error}");
    }
    #[test]
    fn stalled_subprocess_is_terminated() {
        #[cfg(windows)] let mut command = { let mut c = Command::new("powershell"); c.args(["-NoProfile", "-Command", "Start-Sleep -Seconds 30"]); c };
        #[cfg(unix)] let mut command = shell("sleep 30");
        let start = Instant::now();
        assert!(matches!(capture(&mut command, Duration::from_millis(100), "stalled transfer"), Err(CoreError::Timeout(_))));
        assert!(start.elapsed() < Duration::from_secs(10));
    }
}
