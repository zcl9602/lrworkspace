use super::*;
use std::process::Command;

struct TestDir(PathBuf);
impl TestDir {
    fn new()->Self {let root=std::env::temp_dir().join(format!("lrw-array-test-{}",Uuid::new_v4()));std::fs::create_dir_all(&root).unwrap();Self(root)}
    fn posix(&self)->PathBuf {
        let path=self.0.to_string_lossy().replace('\\',"/");
        #[cfg(windows)] let path=format!("/{}{}",path[..1].to_ascii_lowercase(),&path[2..]);
        PathBuf::from(path)
    }
}
impl Drop for TestDir {fn drop(&mut self){let _=std::fs::remove_dir_all(&self.0);}}
fn bash()->Option<PathBuf> {
    let paths=[
        #[cfg(windows)] PathBuf::from(r"C:\Program Files\Git\bin\bash.exe"),
        PathBuf::from("bash"),
    ];
    paths.into_iter().find(|path|Command::new(path).args(["-c","exit 0"]).status().is_ok_and(|status|status.success()))
}
fn plan(commands:usize,scheduler:ClusterScheduler,mut options:BatchOptions)->BatchPlan {
    options.shuffle_seed=Some(42);
    if scheduler==ClusterScheduler::Pbs && options.pbs_flavor.is_none(){options.pbs_flavor=Some(PbsFlavor::PbsPro);}
    let commands=(0..commands).map(|index|BatchCommand {source_line:index+1,command:format!("echo command-{index}")}).collect::<Vec<_>>();
    BatchPlan::build(&commands,&options,scheduler,"JOBS/run.test".into(),Path::new("/work/JOBS/run.test")).unwrap()
}

#[test]
fn defaults_match_the_supplied_perl_instead_of_cluster_resource_maxima() {
    let plan=plan(7,ClusterScheduler::Slurm,BatchOptions::default());
    assert_eq!((plan.summary.array_tasks,plan.summary.array_parallel,plan.summary.cpu_per_task),(7,7,1));
    let script=String::from_utf8(plan.files.iter().find(|file|file.path.ends_with(".slurm")).unwrap().contents.clone()).unwrap();
    assert!(script.contains("#SBATCH --partition=cpu64,cpu128"));
    assert!(script.contains("#SBATCH --array=1-7%7"));
    assert!(script.contains("#SBATCH --nodes=1\n#SBATCH --ntasks=1\n#SBATCH --cpus-per-task=1"));
    assert!(!script.contains("#SBATCH --mem="));assert!(!script.contains("#SBATCH --time="));
}

#[test]
fn parser_filters_comments_and_blank_lines_and_keeps_source_line_numbers() {
    let commands=parse_command_file("\u{feff}#!/bin/bash\r\n\n  # skip\r\necho one\r\n  echo two\n").unwrap();
    assert_eq!(commands.len(),2);assert_eq!(commands[0].source_line,4);
    assert_eq!(commands[1].command,"  echo two");
    assert!(parse_command_file("# nothing\n\n").is_err());
    assert!(parse_command_file("sbatch other.slurm").is_err());
}

#[test]
fn distribution_is_balanced_complete_reproducible_and_cpu_bounded() {
    let options=BatchOptions {ntasks:2,cpu:4,maxnum:300,maxcpu:2000,..Default::default()};
    let first=plan(2501,ClusterScheduler::Slurm,options.clone());
    let second=plan(2501,ClusterScheduler::Slurm,options);
    assert_eq!((first.summary.array_tasks,first.summary.array_parallel),(1000,250));
    let files:Vec<_>=first.files.iter().filter(|file|file.path.contains("/cmd/")).collect();
    assert_eq!(files.len(),1000);
    let provenance=first.files.iter().find(|file|file.path.ends_with("commands.tsv")).unwrap();
    assert_eq!(provenance.contents,second.files.iter().find(|file|file.path.ends_with("commands.tsv")).unwrap().contents);
    let text=String::from_utf8(provenance.contents.clone()).unwrap();
    let mut original:Vec<usize>=text.lines().skip(1).map(|line|line.split('\t').nth(1).unwrap().parse().unwrap()).collect();
    original.sort();assert_eq!(original,(1..=2501).collect::<Vec<_>>());
    let text=String::from_utf8(first.files.iter().find(|file|file.path.ends_with("manifest.tsv")).unwrap().contents.clone()).unwrap();
    let sizes:Vec<usize>=text.lines().skip(1).map(|line|line.split('\t').nth(3).unwrap().parse().unwrap()).collect();
    assert_eq!(sizes.iter().sum::<usize>(),2501);assert_eq!(sizes.iter().min(),Some(&2));assert_eq!(sizes.iter().max(),Some(&3));
}

#[test]
fn invalid_resource_values_and_header_injection_fail_before_submission() {
    for options in [
        BatchOptions {cpu:0,..Default::default()},BatchOptions {maxcpu:1,cpu:2,..Default::default()},
        BatchOptions {nodes:2,ntasks:1,..Default::default()},BatchOptions {jobname:"x\n#SBATCH --exclusive".into(),..Default::default()},
        BatchOptions {partition:Some("cpu;touch bad".into()),..Default::default()},BatchOptions {mem:Some("4G\n".into()),..Default::default()},
    ] {
        assert!(BatchPlan::build(&parse_command_file("echo ok").unwrap(),&options,ClusterScheduler::Slurm,"JOBS/run".into(),Path::new("/work/JOBS/run")).is_err());
    }
    assert!(new_run_relative(&BatchOptions {outdir:"../other".into(),..Default::default()}).is_err());
    let options=BatchOptions::default();assert_ne!(new_run_relative(&options).unwrap(),new_run_relative(&options).unwrap());
    assert!(ensure_plan_budget(usize::MAX,u64::MAX).is_err());
    assert!(parse_command_file(&"echo ok\n".repeat(300_000)).is_err());
}

#[test]
fn pbs_dialects_preserve_total_tasks_threads_and_throttle() {
    for flavor in [PbsFlavor::PbsPro,PbsFlavor::Torque] {
        let options=BatchOptions {pbs_flavor:Some(flavor),nodes:2,ntasks:3,cpu:2,maxnum:100,maxcpu:30,mem:Some("4G".into()),time:Some("1-01:00:00".into()),..Default::default()};
        let plan=plan(9,ClusterScheduler::Pbs,options);
        assert_eq!(plan.summary.array_parallel,5);
        let script=String::from_utf8(plan.files.iter().find(|file|file.path.ends_with(".pbs")).unwrap().contents.clone()).unwrap();
        if flavor==PbsFlavor::PbsPro {
            assert!(script.contains("#PBS -J 1-9%5"));
            assert!(script.contains("1:ncpus=4:mpiprocs=2:ompthreads=2:mem=4294967296b+1:ncpus=2:mpiprocs=1:ompthreads=2:mem=4294967296b"));
        } else {assert!(script.contains("#PBS -t 1-9%5"));assert!(script.contains("nodes=1:ppn=4+1:ppn=2"));assert!(script.contains("mem=8589934592b"));}
        assert!(script.contains("walltime=25:00:00"));
    }
    assert_eq!(PbsFlavor::detect("pbs_version = 23.06.06"),Some(PbsFlavor::PbsPro));
    assert_eq!(PbsFlavor::detect("TORQUE version 6.1"),Some(PbsFlavor::Torque));
    assert!(PbsFlavor::detect("unknown build").is_none());
    assert_eq!(pbs_walltime("1-02").unwrap(),"26:00:00");
    assert_eq!(pbs_walltime("1-02:30").unwrap(),"26:30:00");
    assert_eq!(pbs_walltime("150").unwrap(),"02:30:00");
    let single=plan(1,ClusterScheduler::Pbs,BatchOptions::default());
    let script=String::from_utf8(single.files.iter().find(|file|file.path.ends_with(".pbs")).unwrap().contents.clone()).unwrap();
    assert!(!script.contains("#PBS -J"));assert!(script.contains("TASK_ID=1"));
}

#[test]
fn failed_command_does_not_prevent_later_commands_and_returns_failure() {
    let Some(bash)=bash() else{return};let directory=TestDir::new();
    let marker=normalize_remote_absolute(&directory.posix().join("marker")).unwrap();
    let commands=vec![BatchCommand {source_line:1,command:format!("printf first >> {}",shell_quote(&marker))},BatchCommand {source_line:2,command:"false".into()},BatchCommand {source_line:3,command:format!("printf last >> {}",shell_quote(&marker))}];
    let script=directory.0.join("command.sh");std::fs::write(&script,command_script(&commands,0,3)).unwrap();
    let output=Command::new(&bash).arg(&script).env("LRW_STATUS_FILE",directory.posix().join("status.tsv")).output().unwrap();
    assert_eq!(output.status.code(),Some(1));assert_eq!(std::fs::read(directory.0.join("marker")).unwrap(),b"firstlast");
    assert!(String::from_utf8(output.stdout).unwrap().contains("Failed: 1 / 3"));
    assert_eq!(std::fs::read_to_string(directory.0.join("status.tsv")).unwrap(),"1\t0\n2\t1\n3\t0\n");
    let output=Command::new(&bash).arg(&script).env_remove("LRW_STATUS_FILE").output().unwrap();
    assert_eq!(output.status.code(),Some(1));
    assert_eq!(std::fs::read(directory.0.join("marker")).unwrap(),b"firstlastfirstlast");
}

#[test]
fn real_tar_and_bash_execute_all_dispatcher_dialects_without_deleting_old_runs() {
    let Some(bash)=bash() else{return};let directory=TestDir::new();
    std::fs::write(directory.0.join("previous-result"),b"preserve").unwrap();
    for (index,scheduler,flavor) in [(0,ClusterScheduler::Slurm,None),(1,ClusterScheduler::Pbs,Some(PbsFlavor::PbsPro)),(2,ClusterScheduler::Pbs,Some(PbsFlavor::Torque))] {
        let run=directory.0.join(format!("run{index}"));std::fs::create_dir_all(run.join("array.0001/out")).unwrap();std::fs::create_dir_all(run.join("array.0001/err")).unwrap();
        let posix=directory.posix().join(format!("run{index}"));
        let plan=BatchPlan::build(&parse_command_file("printf success").unwrap(),&BatchOptions {pbs_flavor:flavor,..Default::default()},scheduler,format!("run{index}").into(),&posix).unwrap();
        let archive=run.join("bundle.tar");std::fs::write(&archive,plan.archive().unwrap()).unwrap();
        let output=Command::new("tar").arg("-xf").arg(&archive).arg("-C").arg(&run).output().unwrap();
        assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
        let script=run.join(plan.summary.script_path.rsplit('/').next().unwrap());
        let script=run.join("array.0001").join(script.file_name().unwrap());
        let syntax=Command::new(&bash).arg("-n").arg(&script).output().unwrap();
        assert!(syntax.status.success(),"{}",String::from_utf8_lossy(&syntax.stderr));
        for _attempt in 0..2 {
            let output=Command::new(&bash).arg(&script)
                .env("SLURM_SUBMIT_DIR",directory.posix()).env("SLURM_ARRAY_TASK_ID","1").env("SLURM_JOB_ID","123").env("SLURM_ARRAY_JOB_ID","123")
                .env("PBS_O_WORKDIR",directory.posix()).env("PBS_JOBID","123[1].server").env("PBS_ARRAYID","1").output().unwrap();
            assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
        }
        let logs:Vec<_>=std::fs::read_dir(run.join("array.0001/out")).unwrap().map(Result::unwrap).filter(|entry|entry.path().extension().is_some_and(|ext|ext=="out")).collect();
        assert_eq!(logs.len(),2,"attempt logs must be preserved");
        assert!(std::fs::read_to_string(logs[0].path()).unwrap().contains("success"));
    }
    assert_eq!(std::fs::read(directory.0.join("previous-result")).unwrap(),b"preserve");
}

#[test]
#[ignore="requires local Perl/Bash; runs the supplied reference only in a fresh fixture and never submits"]
fn reference_perl_and_native_array_contract_match() {
    let Some(bash)=bash() else{panic!("Bash is required for explicit reference parity")};
    let target=std::env::var_os("CARGO_TARGET_DIR").map(PathBuf::from)
        .unwrap_or_else(||Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap().join("target"));
    let root=target.join("tmp").join(format!("lrw-perl-parity-{}",Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();let directory=TestDir(root);
    let commands=(0..2501).map(|index|format!("echo command-{index}\n")).collect::<String>();
    std::fs::write(directory.0.join("commands.sh"),&commands).unwrap();
    let source=Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap().join("docs/reference/slurm_array_user.pl");
    // Keep the authoritative archive byte-identical; only the disposable test
    // copy gets portable LF line endings for Perl on Unix as well as Windows.
    std::fs::write(directory.0.join("reference.pl"),std::fs::read_to_string(source).unwrap().replace("\r\n","\n")).unwrap();
    let source=normalize_remote_absolute(&directory.posix().join("reference.pl")).unwrap();
    let input=normalize_remote_absolute(&directory.posix().join("commands.sh")).unwrap();
    let outdir=normalize_remote_absolute(&directory.posix().join("perl-output")).unwrap();
    let command=format!("perl {} --in {} --outdir {} --ntasks 2 --cpu 4 --maxnum 300 --maxcpu 2000 --jobname test --no-submit",shell_quote(&source),shell_quote(&input),shell_quote(&outdir));
    let output=Command::new(bash).args(["-c",&command]).output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    let manifest=std::fs::read_to_string(directory.0.join("perl-output/array.0001/manifest.tsv")).unwrap();
    let counts:Vec<usize>=manifest.lines().skip(1).map(|line|line.split('\t').nth(3).unwrap().parse().unwrap()).collect();
    let native=plan(2501,ClusterScheduler::Slurm,BatchOptions {ntasks:2,cpu:4,maxnum:300,maxcpu:2000,jobname:"test".into(),..Default::default()});
    let native_manifest=String::from_utf8(native.files.iter().find(|file|file.path.ends_with("manifest.tsv")).unwrap().contents.clone()).unwrap();
    let native_counts:Vec<usize>=native_manifest.lines().skip(1).map(|line|line.split('\t').nth(3).unwrap().parse().unwrap()).collect();
    assert_eq!(counts,native_counts);assert_eq!(counts.iter().sum::<usize>(),2501);
    let script=std::fs::read_to_string(directory.0.join("perl-output/array.0001/test.array.slurm")).unwrap();
    for directive in ["--partition=cpu64,cpu128","--nodes=1","--ntasks=2","--cpus-per-task=4","--array=1-1000%250"] {assert!(script.contains(directive));}
    assert_eq!(native.summary.array_parallel,250);assert!(!script.contains("#SBATCH --mem="));assert!(!script.contains("#SBATCH --time="));
    let mut original=Vec::new();
    for file in std::fs::read_dir(directory.0.join("perl-output/array.0001/cmd")).unwrap() {
        let text=std::fs::read_to_string(file.unwrap().path()).unwrap();
        original.extend(text.lines().filter_map(|line|line.strip_prefix("echo command-")).map(|number|number.parse::<usize>().unwrap()));
    }
    original.sort();assert_eq!(original,(0..2501).collect::<Vec<_>>());
}

#[test]
fn streaming_archive_matches_existing_contract_and_propagates_disk_errors() {
    let commands=parse_command_file("echo first\necho 'quoted'\nfalse\necho last").unwrap();
    let options=BatchOptions{shuffle_seed:Some(99),..Default::default()};
    let plan=BatchPlan::build(&commands,&options,ClusterScheduler::Slurm,"JOBS/run".into(),Path::new("/work/JOBS/run")).unwrap();
    let mut streamed=Vec::new();BatchPlan::write_archive(&commands,&plan.summary,"",&mut streamed).unwrap();
    assert_eq!(streamed,plan.archive().unwrap());
    let prefix="cd '/work/space dir' || exit 1\n";
    let wrapped:Vec<_>=commands.iter().map(|c|BatchCommand{source_line:c.source_line,command:format!("{prefix}{}",c.command)}).collect();
    let wrapped_plan=BatchPlan::build(&wrapped,&options,ClusterScheduler::Slurm,"JOBS/run".into(),Path::new("/work/JOBS/run")).unwrap();
    let mut prefixed=Vec::new();BatchPlan::write_archive(&commands,&plan.summary,prefix,&mut prefixed).unwrap();
    assert_eq!(prefixed,wrapped_plan.archive().unwrap());
    struct Broken;
    impl std::io::Write for Broken {
        fn write(&mut self,_:&[u8])->std::io::Result<usize>{Err(std::io::Error::other("disk full"))}
        fn flush(&mut self)->std::io::Result<()>{Ok(())}
    }
    assert!(BatchPlan::write_archive(&commands,&plan.summary,"",&mut Broken).is_err());
}

#[test]
fn torque_spool_paths_stay_inside_log_directories_without_quotes() {
    let plan=plan(2,ClusterScheduler::Pbs,BatchOptions{pbs_flavor:Some(PbsFlavor::Torque),..Default::default()});
    let script=String::from_utf8(plan.files.iter().find(|f|f.path.ends_with(".pbs")).unwrap().contents.clone()).unwrap();
    let (out,err)=pbs_log_paths(&plan.summary);
    assert!(out.ends_with("/out/scheduler") && err.ends_with("/err/scheduler"));
    assert!(script.contains(&format!("#PBS -o {out}\n")));assert!(!script.contains("#PBS -o \""));
    let spaced=BatchPlan::build(&parse_command_file("echo ok").unwrap(),&BatchOptions{pbs_flavor:Some(PbsFlavor::Torque),..Default::default()},ClusterScheduler::Pbs,"run".into(),Path::new("/work/space dir/run")).unwrap();
    let script=String::from_utf8(spaced.files.iter().find(|f|f.path.ends_with(".pbs")).unwrap().contents.clone()).unwrap();
    assert!(!script.contains("#PBS -o "));assert!(script.contains("qsub arguments"));
}

#[test]
fn single_torque_group_uses_a_regular_job_with_the_same_dispatcher() {
    let plan=plan(1,ClusterScheduler::Pbs,BatchOptions{pbs_flavor:Some(PbsFlavor::Torque),..Default::default()});
    let script=String::from_utf8(plan.files.iter().find(|f|f.path.ends_with(".pbs")).unwrap().contents.clone()).unwrap();
    assert!(!script.contains("#PBS -t "));assert!(script.contains("TASK_ID=1\n"));assert_eq!(plan.summary.array_tasks,1);
}
