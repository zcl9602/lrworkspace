//! Native implementation of the user's shuffled, bounded command-array mode.
//! A fresh run directory is used instead of deleting any existing cmd/out/err.
use std::path::{Path, PathBuf};
use lrworkspace_core::{CoreError, CoreResult};
use lrworkspace_core::permission::{classify_command_risk, DefaultPolicy};
use lrworkspace_core::safety::{normalize_remote_absolute, safe_join, shell_quote};
use lrworkspace_core::types::ClusterScheduler;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MAX_ARRAY_TASKS: usize = 1000;
pub const MAX_INPUT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ESTIMATED_PLAN_BYTES:u64=256*1024*1024;

fn ensure_plan_budget(count:usize,bytes:u64)->CoreResult<()> {
    let estimate=(count as u64).checked_mul(1024).and_then(|overhead|bytes.checked_mul(8).and_then(|payload|overhead.checked_add(payload)));
    if estimate.is_none_or(|estimate|estimate>MAX_ESTIMATED_PLAN_BYTES) {
        return Err(CoreError::Validation("Estimated generated command plan exceeds 256 MiB; split the command file into smaller batches".into()));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all="kebab-case")]
pub enum PbsFlavor { PbsPro, Torque }

impl PbsFlavor {
    pub fn parse(value: &str) -> CoreResult<Self> {
        match value { "pbs-pro" | "openpbs" => Ok(Self::PbsPro), "torque" => Ok(Self::Torque),
            _ => Err(CoreError::Validation("pbs_flavor must be pbs-pro or torque".into())) }
    }
    pub fn detect(version: &str) -> Option<Self> {
        let value=version.to_ascii_lowercase();
        // Torque's --version commonly prints only "Version: ..." on stderr.
        let torque=value.contains("torque") || value.lines().any(|line|
            line.trim().strip_prefix("version:").is_some_and(|number|number.trim().starts_with(|c:char|c.is_ascii_digit())));
        // An old numeric pbs_version by itself is shared by multiple PBS
        // families; recent PBS Pro/OpenPBS releases use 10+ release numbers.
        let modern_pbs=value.lines().any(|line|line.trim().strip_prefix("pbs_version")
            .and_then(|tail|tail.trim().strip_prefix('='))
            .and_then(|number|number.trim().split('.').next()?.parse::<u32>().ok()).is_some_and(|major|major>=10));
        let pro=value.contains("openpbs") || value.contains("pbspro") || value.contains("pbs professional") || modern_pbs;
        match (pro,torque) {
            (true,false)=>Some(Self::PbsPro),
            (false,true)=>Some(Self::Torque),
            (false,false)=>{
                // Querying usage is safe and distinguishes array syntax even
                // when a site's client wrapper omits its version identifier.
                if !value.contains("usage:") || !value.contains("qsub") {return None;}
                let flag=|name:&str|regex::Regex::new(&format!(r"(?:^|[\s\[|])-{}(?:[\s\]>]|$)",name)).expect("fixed option regex").is_match(version);
                match (flag("J"),flag("t")) {
                    (true,false)=>Some(Self::PbsPro), (false,true)=>Some(Self::Torque), _=>None,
                }
            }
            _=>None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BatchOptions {
    pub outdir: PathBuf,
    pub partition: Option<String>,
    pub queue: Option<String>,
    pub nodes: u32,
    pub ntasks: u32,
    pub cpu: u32,
    pub maxnum: u32,
    pub maxcpu: u64,
    pub jobname: String,
    pub time: Option<String>,
    pub mem: Option<String>,
    pub shuffle_seed: Option<u64>,
    pub pbs_flavor: Option<PbsFlavor>,
}

impl Default for BatchOptions {
    fn default() -> Self {
        Self { outdir:"._lrworkspace_jobs".into(), partition:None, queue:None,
            nodes:1, ntasks:1, cpu:1, maxnum:1000, maxcpu:2000, jobname:"job".into(),
            time:None, mem:None, shuffle_seed:None, pbs_flavor:None }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCommand { pub source_line: usize, pub command: String }

pub fn parse_command_file(text: &str) -> CoreResult<Vec<BatchCommand>> {
    if text.len() as u64 > MAX_INPUT_BYTES { return Err(CoreError::Validation("Command file exceeds 64 MiB".into())); }
    let (count,bytes)=text.trim_start_matches('\u{feff}').lines().filter(|line|!line.trim().is_empty()&&!line.trim_start().starts_with('#'))
        .fold((0usize,0u64),|(count,bytes),line|(count+1,bytes+line.len() as u64));
    ensure_plan_budget(count,bytes)?;
    let commands: Vec<_> = text.trim_start_matches('\u{feff}').lines().enumerate()
        .filter_map(|(line,text)| {
            let text=text.trim_end_matches('\r');
            if text.trim().is_empty() || text.trim_start().starts_with('#') { None }
            else { Some(BatchCommand { source_line:line+1, command:text.into() }) }
        }).collect();
    validate_commands(&commands)?;
    Ok(commands)
}

pub fn read_command_file(path: &Path) -> CoreResult<Vec<BatchCommand>> {
    use std::io::Read;
    let file=std::fs::File::open(path)?;
    if !file.metadata()?.is_file() { return Err(CoreError::Validation("Command input must be a regular file".into())); }
    let mut text=String::new();
    file.take(MAX_INPUT_BYTES+1).read_to_string(&mut text)?;
    parse_command_file(&text)
}

pub fn validate_commands(commands: &[BatchCommand]) -> CoreResult<()> {
    if commands.is_empty() { return Err(CoreError::Validation("No valid commands found".into())); }
    ensure_plan_budget(commands.len(),commands.iter().fold(0u64,|size,command|size.saturating_add(command.command.len() as u64)))?;
    for command in commands {
        if command.command.trim().is_empty() || command.command.contains('\0') { return Err(CoreError::Validation("Commands must be nonempty and contain no NUL".into())); }
        if matches!(classify_command_risk(&command.command).default_policy(), DefaultPolicy::Confirm | DefaultPolicy::Deny) {
            return Err(CoreError::PermissionDenied(format!("Refused command at source line {}",command.source_line)));
        }
        let program=command.command.split_whitespace().next().unwrap_or("");
        if matches!(program.trim_matches(['\'','"']).rsplit('/').next().unwrap_or(""),"sbatch"|"qsub") {
            return Err(CoreError::Validation("Command files contain workloads, not nested sbatch/qsub calls".into()));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSummary {
    pub run_id: String,
    pub scheduler: ClusterScheduler,
    pub pbs_flavor: Option<PbsFlavor>,
    pub command_count: usize,
    pub array_tasks: usize,
    pub array_parallel: usize,
    pub cpu_per_task: u64,
    pub shuffle_seed: u64,
    pub run_relative: PathBuf,
    pub run_directory: String,
    pub script_relative: PathBuf,
    pub script_path: String,
    pub manifest_path: String,
    pub stdout_directory: String,
    pub stderr_directory: String,
    pub options: BatchOptions,
}

#[derive(Debug, Clone)]
pub struct BatchFile { pub path: String, pub contents: Vec<u8>, pub mode: u32 }

#[derive(Debug, Clone)]
pub struct BatchPlan { pub summary: BatchSummary, pub files: Vec<BatchFile> }

pub fn new_run_relative(options: &BatchOptions) -> CoreResult<PathBuf> {
    // Validation is relative to a dummy POSIX work root and cannot create files.
    let parent=normalize_remote_absolute(&safe_join(Path::new("/work"), &options.outdir)?)?;
    let parent=parent.strip_prefix("/work").expect("validated root").trim_start_matches('/');
    Ok(PathBuf::from(format!("{}/run.{}",parent,Uuid::new_v4().simple()).trim_start_matches('/')))
}

impl BatchPlan {
    pub fn prepare(commands: &[BatchCommand], options: &BatchOptions, scheduler: ClusterScheduler,
        run_relative: PathBuf, run_directory: &Path) -> CoreResult<BatchSummary> {
        let mut options=options.clone();
        if scheduler==ClusterScheduler::Slurm && options.partition.is_none() {options.partition=Some("cpu64,cpu128".into());}
        validate_commands(commands)?;
        if !matches!(scheduler, ClusterScheduler::Slurm|ClusterScheduler::Pbs) { return Err(CoreError::InvalidState("Batch arrays require Slurm or PBS".into())); }
        let cpu_per_task=(options.ntasks as u64).checked_mul(options.cpu as u64)
            .ok_or_else(||CoreError::Validation("CPU requirement overflow".into()))?;
        if options.nodes==0 || options.ntasks==0 || options.cpu==0 || options.maxnum==0 || options.maxcpu==0 {
            return Err(CoreError::Validation("nodes, ntasks, cpu, maxnum and maxcpu must be >= 1".into()));
        }
        if options.nodes>options.ntasks { return Err(CoreError::Validation("ntasks must be >= nodes to allocate at least one task per node".into())); }
        if options.maxcpu<cpu_per_task { return Err(CoreError::Validation("maxcpu is smaller than the CPU requirement of one task".into())); }
        if options.jobname.is_empty() || options.jobname.len()>64 || !options.jobname.bytes().all(|b|b.is_ascii_alphanumeric()||b"_.-".contains(&b)) {
            return Err(CoreError::Validation("jobname must contain 1-64 ASCII letters, digits, _, . or -".into()));
        }
        if scheduler==ClusterScheduler::Pbs && (options.jobname.len()>15 || !options.jobname.as_bytes()[0].is_ascii_alphabetic()) {
            return Err(CoreError::Validation("Portable PBS jobnames start with a letter and have at most 15 ASCII characters".into()));
        }
        for (name,value) in [("partition",options.partition.as_deref()),("queue",options.queue.as_deref())] {
            if let Some(value)=value { if value.is_empty() || !value.bytes().all(|b|b.is_ascii_alphanumeric()||b"_.-,@".contains(&b)) {
                return Err(CoreError::Validation(format!("Invalid {name}")));
            }}
        }
        if scheduler==ClusterScheduler::Pbs && options.queue.as_deref().or(options.partition.as_deref()).is_some_and(|queue|queue.contains(',')) {
            return Err(CoreError::Validation("PBS requires one queue, not a Slurm partition list".into()));
        }
        if (scheduler==ClusterScheduler::Slurm && options.queue.is_some()) || (options.queue.is_some() && options.partition.is_some()) {
            return Err(CoreError::Validation("Use partition for Slurm, queue for PBS; do not supply both".into()));
        }
        if options.time.as_deref().is_some_and(|value| value.is_empty() || !value.bytes().all(|b|b.is_ascii_digit()||b":-".contains(&b))) { return Err(CoreError::Validation("Invalid time value".into())); }
        if let Some(memory)=options.mem.as_deref() { memory_bytes(memory)?; }
        let pbs_flavor=if scheduler==ClusterScheduler::Pbs { Some(options.pbs_flavor.ok_or_else(||CoreError::Validation("Specify/detect pbs_flavor before generating a PBS array".into()))?) } else { None };
        let array_tasks=commands.len().min(MAX_ARRAY_TASKS);
        let array_parallel=(array_tasks as u64).min(options.maxnum as u64).min(options.maxcpu/cpu_per_task) as usize;
        let run_directory=normalize_remote_absolute(run_directory)?;
        if run_directory.contains(['\n','\r']) { return Err(CoreError::Validation("Run directory cannot contain newlines".into())); }
        let array=format!("{}/array.0001",run_directory.trim_end_matches('/'));
        let extension=if scheduler==ClusterScheduler::Slurm { "slurm" } else { "pbs" };
        let script_name=format!("{}.array.{extension}", options.jobname);
        let seed=options.shuffle_seed.unwrap_or_else(|| {
            let random=Uuid::new_v4(); u64::from_le_bytes(random.as_bytes()[..8].try_into().expect("eight bytes"))
        });
        let summary=BatchSummary { run_id:run_relative.file_name().unwrap_or_default().to_string_lossy().into(), scheduler,
            pbs_flavor, command_count:commands.len(), array_tasks,array_parallel,cpu_per_task,shuffle_seed:seed,
            script_relative:PathBuf::from(format!("{}/array.0001/{script_name}",run_relative.to_string_lossy().replace('\\',"/"))), script_path:format!("{array}/{script_name}"),
            manifest_path:format!("{array}/manifest.tsv"), stdout_directory:format!("{array}/out"),stderr_directory:format!("{array}/err"),
            run_relative,run_directory, options:options.clone() };
        Ok(summary)
    }

    pub fn build(commands:&[BatchCommand],options:&BatchOptions,scheduler:ClusterScheduler,run_relative:PathBuf,run_directory:&Path)->CoreResult<Self> {
        let summary=Self::prepare(commands,options,scheduler,run_relative,run_directory)?;
        let mut files=Vec::new();
        generate_files(commands,&summary,"",|file|{files.push(file);Ok(())})?;
        Ok(Self {summary,files})
    }

    /// Production path retains one generated member at a time, not a full TAR.
    pub fn write_archive(commands:&[BatchCommand],summary:&BatchSummary,prefix:&str,writer:&mut impl std::io::Write)->CoreResult<()> {
        if commands.len()!=summary.command_count || summary.array_tasks==0 || summary.array_tasks>commands.len(){return Err(CoreError::Validation("Commands do not match the prepared batch summary".into()));}
        let bytes=commands.iter().fold(0u64,|n,c|n.saturating_add(c.command.len() as u64).saturating_add(prefix.len() as u64));
        ensure_plan_budget(commands.len(),bytes)?;
        generate_files(commands,summary,prefix,|file|append_archive_member(writer,&file))?;
        writer.write_all(&[0u8;1024])?;
        Ok(())
    }

    /// Compatibility helper for library callers/tests that need an in-memory TAR.
    pub fn archive(&self)->CoreResult<Vec<u8>> {
        let mut archive=Vec::new();
        for file in &self.files {append_archive_member(&mut archive,file)?;}
        archive.extend_from_slice(&[0;1024]);Ok(archive)
    }
}

fn generate_files(commands:&[BatchCommand],summary:&BatchSummary,prefix:&str,mut emit:impl FnMut(BatchFile)->CoreResult<()>)->CoreResult<()> {
    let mut shuffled:Vec<usize>=(0..commands.len()).collect();shuffle(&mut shuffled,summary.shuffle_seed);
    let mut manifest=String::from("task_id\tcommand_start\tcommand_end\tcommand_count\tcommand_file\n");
    let base=commands.len()/summary.array_tasks;let remainder=commands.len()%summary.array_tasks;let mut position=0;
    for task_id in 1..=summary.array_tasks {
        let count=base+usize::from(task_id<=remainder);let name=format!("array.0001/cmd/cmd.{task_id:04}.sh");
        let script=command_script_iter(shuffled[position..position+count].iter().map(|i|&commands[*i]),position,commands.len(),prefix);
        manifest.push_str(&format!("{task_id}\t{}\t{}\t{count}\t{}/{}\n",position+1,position+count,summary.run_directory,name));
        emit(BatchFile {path:name,contents:script.into_bytes(),mode:0o755})?;position+=count;
    }
    emit(BatchFile {path:"array.0001/manifest.tsv".into(),contents:manifest.into_bytes(),mode:0o644})?;
    let mut provenance=String::from("command_index\tsource_line\tcommand_json\n");
    for (index,id) in shuffled.iter().enumerate() {
        let command=&commands[*id];
        let text=if prefix.is_empty(){std::borrow::Cow::Borrowed(command.command.as_str())}else{std::borrow::Cow::Owned(format!("{prefix}{}",command.command))};
        provenance.push_str(&format!("{}\t{}\t{}\n",index+1,command.source_line,serde_json::to_string(&text)?));
    }
    emit(BatchFile {path:"array.0001/commands.tsv".into(),contents:provenance.into_bytes(),mode:0o644})?;
    let file=summary.script_path.rsplit('/').next().expect("generated script path");
    emit(BatchFile {path:format!("array.0001/{file}"),contents:dispatcher(summary)?.into_bytes(),mode:0o755})?;
    emit(BatchFile {path:"array.0001/plan.json".into(),contents:serde_json::to_vec_pretty(summary)?,mode:0o644})?;
    Ok(())
}

fn append_archive_member(writer:&mut impl std::io::Write,file:&BatchFile)->CoreResult<()> {
    if file.path.len()>100 || file.path.starts_with('/') || file.path.split('/').any(|part|part==".."||part.is_empty()) {
        return Err(CoreError::InvalidState("Unsafe generated archive member".into()));
    }
    let mut header=[0u8;512];header[..file.path.len()].copy_from_slice(file.path.as_bytes());
    octal(&mut header[100..108],file.mode as u64)?;octal(&mut header[108..116],0)?;octal(&mut header[116..124],0)?;
    octal(&mut header[124..136],file.contents.len() as u64)?;octal(&mut header[136..148],0)?;
    header[148..156].fill(b' ');header[156]=b'0';header[257..263].copy_from_slice(b"ustar\0");header[263..265].copy_from_slice(b"00");
    let checksum:u64=header.iter().map(|&b|b as u64).sum();header[148..156].copy_from_slice(format!("{checksum:06o}\0 ").as_bytes());
    writer.write_all(&header)?;writer.write_all(&file.contents)?;
    writer.write_all(&[0u8;512][..(512-file.contents.len()%512)%512])?;
    Ok(())
}

fn octal(target:&mut [u8], value:u64) -> CoreResult<()> {
    let value=format!("{value:0width$o}\0",width=target.len()-1);
    if value.len()!=target.len() { return Err(CoreError::Validation("Archive value is too large".into())); }
    target.copy_from_slice(value.as_bytes()); Ok(())
}

fn shuffle<T>(commands:&mut [T],mut state:u64) {
    // SplitMix64 supplies reproducible ordering, not security randomness.
    for end in (1..commands.len()).rev() {
        state=state.wrapping_add(0x9e3779b97f4a7c15);
        let mut value=state; value=(value^(value>>30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value=(value^(value>>27)).wrapping_mul(0x94d049bb133111eb); value^=value>>31;
        commands.swap(end,(value%((end+1) as u64)) as usize);
    }
}

#[cfg(test)]
fn command_script(commands:&[BatchCommand],start:usize,total:usize)->String {command_script_iter(commands.iter(),start,total,"")}

fn command_script_iter<'a>(commands:impl ExactSizeIterator<Item=&'a BatchCommand>,start:usize,total:usize,prefix:&str)->String {
    let mut script=format!("#!/usr/bin/env bash\nset +e\nFAILED=0\nTOTAL={}\necho \"Commands: {}-{} / {total}\"\necho \"Start: $(date)\"\n",commands.len(),start+1,start+commands.len());
    for (index,command) in commands.enumerate() {
        let global=start+index+1;
        script.push_str(&format!("echo 'Command index: {global} / {total}'\n(\n{prefix}{}\n)\nstatus=$?\nif (( status != 0 )); then\n  echo \"ERROR: command {global} failed with exit status $status\" >&2\n  FAILED=$((FAILED + 1))\nfi\nif [[ -n \"${{LRW_STATUS_FILE:-}}\" ]]; then\nprintf '%s\\t%s\\n' {global} \"$status\" >> \"$LRW_STATUS_FILE\" || {{ echo 'ERROR: cannot record command status' >&2; FAILED=$((FAILED + 1)); }}\nfi\n",command.command));
    }
    script.push_str("echo \"End: $(date)\"\necho \"Failed: $FAILED / $TOTAL\"\nif (( FAILED > 0 )); then exit 1; else exit 0; fi\n"); script
}

fn directive(value:&str) -> String { format!("\"{}\"",value.replace('\\',"\\\\").replace('"',"\\\"")) }

pub fn pbs_log_paths(plan:&BatchSummary)->(String,String) {
    if plan.pbs_flavor==Some(PbsFlavor::Torque) {
        (format!("{}/scheduler",plan.stdout_directory),format!("{}/scheduler",plan.stderr_directory))
    } else {(plan.stdout_directory.clone(),plan.stderr_directory.clone())}
}

fn dispatcher(plan:&BatchSummary) -> CoreResult<String> {
    let options=&plan.options; let array=format!("1-{}%{}",plan.array_tasks,plan.array_parallel);
    let mut script=String::from("#!/usr/bin/env bash\n");
    if plan.scheduler==ClusterScheduler::Slurm {
        script.push_str(&format!("#SBATCH --job-name={}\n#SBATCH --partition={}\n#SBATCH --nodes={}\n#SBATCH --ntasks={}\n#SBATCH --cpus-per-task={}\n#SBATCH --array={array}\n#SBATCH --open-mode=append\n#SBATCH --output={}\n#SBATCH --error={}\n",options.jobname,options.partition.as_deref().unwrap_or("cpu64,cpu128"),options.nodes,options.ntasks,options.cpu,
            directive(&format!("{}/scheduler.%A_%a.out",plan.stdout_directory)),directive(&format!("{}/scheduler.%A_%a.err",plan.stderr_directory))));
        if let Some(time)=&options.time {script.push_str(&format!("#SBATCH --time={time}\n"));}
        if let Some(mem)=&options.mem {script.push_str(&format!("#SBATCH --mem={mem}\n"));}
    } else {
        script.push_str(&format!("#PBS -N {}\n#PBS -S /bin/bash\n#PBS -r y\n",options.jobname));
        let (stdout,stderr)=pbs_log_paths(plan);
        // Old Torque treats directive quotes as literal filename characters.
        // Simple paths also work for standalone scripts; complex paths are
        // supplied as real qsub argv by the native submission backend.
        for (flag,path) in [("o",stdout),("e",stderr)] {
            if path.bytes().all(|b|b.is_ascii_alphanumeric()||b"/_.-".contains(&b)) {
                script.push_str(&format!("#PBS -{flag} {path}\n"));
            } else {script.push_str("# LRW passes scheduler log paths through qsub arguments\n");}
        }
        if let Some(queue)=options.queue.as_deref().or(options.partition.as_deref()) { script.push_str(&format!("#PBS -q {queue}\n")); }
        match plan.pbs_flavor.expect("PBS flavor resolved") {
            PbsFlavor::PbsPro => {
                // PBS Pro requires end > start; a one-task plan is a regular
                // job running the same dispatcher and command-file format.
                if plan.array_tasks>1 {script.push_str(&format!("#PBS -J {array}\n"));}
                script.push_str(&format!("#PBS -l select={}\n",pbs_chunks(options,true)?));
                if options.nodes>1 {script.push_str("#PBS -l place=scatter\n");}
            }
            PbsFlavor::Torque => {
                if plan.array_tasks>1 {script.push_str(&format!("#PBS -t {array}\n"));}
                script.push_str(&format!("#PBS -l nodes={}\n",pbs_chunks(options,false)?));
                if let Some(memory)=&options.mem {
                    let bytes=memory_bytes(memory)?.checked_mul(options.nodes as u64).ok_or_else(||CoreError::Validation("Total PBS memory overflow".into()))?;
                    script.push_str(&format!("#PBS -l mem={bytes}b\n"));
                }
            }
        }
        if let Some(time)=&options.time {script.push_str(&format!("#PBS -l walltime={}\n",pbs_walltime(time)?));}
    }
    let cmd_dir=format!("{}/array.0001/cmd",plan.run_directory);
    script.push_str("\nset -u\n");
    if plan.scheduler==ClusterScheduler::Slurm {
        script.push_str("cd -- \"${SLURM_SUBMIT_DIR:?SLURM_SUBMIT_DIR is missing}\" || exit 1\nTASK_ID=${SLURM_ARRAY_TASK_ID:?}\nJOB_ID=${SLURM_ARRAY_JOB_ID:-${SLURM_JOB_ID:?}}\n");
    } else {
        script.push_str("cd -- \"${PBS_O_WORKDIR:?PBS_O_WORKDIR is missing}\" || exit 1\nJOB_ID=${PBS_JOBID:?}\n");
        if plan.array_tasks==1 {script.push_str("TASK_ID=1\n");}
        else {script.push_str("TASK_ID=${PBS_ARRAY_INDEX:-${PBS_ARRAYID:-}}\n");}
    }
    script.push_str(&format!("[[ $TASK_ID =~ ^[0-9]+$ ]] || {{ echo 'Invalid array task index' >&2; exit 1; }}\nTASK_ID=$((10#$TASK_ID))\n(( TASK_ID >= 1 && TASK_ID <= {} )) || exit 1\ncase \"$JOB_ID\" in *[!a-zA-Z0-9_.\\[\\]-]*|'') echo 'Invalid job ID' >&2; exit 1;; esac\nCMD_DIR={}\nOUT_DIR={}\nERR_DIR={}\nprintf -v TAG '%04d' \"$TASK_ID\"\nCMD_FILE=\"$CMD_DIR/cmd.$TAG.sh\"\n[[ -r \"$CMD_FILE\" ]] || {{ echo \"Missing command file: $CMD_FILE\" >&2; exit 1; }}\nATTEMPT=\"$(date +%s)-$$\"\nset -C\nexec > \"$OUT_DIR/{}.$JOB_ID-$TASK_ID.$ATTEMPT.out\" 2> \"$ERR_DIR/{}.$JOB_ID-$TASK_ID.$ATTEMPT.err\"\nexport LRW_STATUS_FILE=\"$OUT_DIR/status.$JOB_ID-$TASK_ID.$ATTEMPT.tsv\"\nprintf 'command_index\\texit_status\\n' > \"$LRW_STATUS_FILE\" || exit 1\nexport OMP_NUM_THREADS=${{OMP_NUM_THREADS:-{}}}\necho \"Job ID: $JOB_ID\"\necho \"Array Task ID: $TASK_ID\"\necho \"Node: $(hostname)\"\necho \"Command file: $CMD_FILE\"\necho \"Start: $(date)\"\nbash \"$CMD_FILE\"\nstatus=$?\necho \"End: $(date)\"\necho \"Exit status: $status\"\nexit \"$status\"\n",
        plan.array_tasks,shell_quote(&cmd_dir),shell_quote(&plan.stdout_directory),shell_quote(&plan.stderr_directory),options.jobname,options.jobname,options.cpu));
    Ok(script)
}

fn pbs_chunks(options:&BatchOptions,pro:bool) -> CoreResult<String> {
    let base=options.ntasks/options.nodes; let extra=options.ntasks%options.nodes;
    let mut chunks=Vec::new();
    let mut groups=Vec::new();
    if extra>0 {groups.push((extra,base+1));}
    groups.push((options.nodes-extra,base));
    for (count,tasks) in groups {
        let ncpus=(tasks as u64)*(options.cpu as u64);
        let mut chunk=if pro {format!("{count}:ncpus={ncpus}:mpiprocs={tasks}:ompthreads={}",options.cpu)} else {format!("{count}:ppn={ncpus}")};
        if pro {if let Some(memory)=&options.mem {chunk.push_str(&format!(":mem={}b",memory_bytes(memory)?));}}
        chunks.push(chunk);
    }
    Ok(chunks.join("+"))
}

fn memory_bytes(value:&str) -> CoreResult<u64> {
    let digits=value.bytes().take_while(u8::is_ascii_digit).count();
    let number=value[..digits].parse::<u64>().ok().filter(|number|*number>0);
    let unit=value[digits..].to_ascii_lowercase();
    let power=match unit.as_str() {""|"m"|"mb"=>2,"b"=>0,"k"|"kb"=>1,"g"|"gb"=>3,"t"|"tb"=>4,"p"|"pb"=>5,_=>return Err(CoreError::Validation("Invalid memory unit; use e.g. 4G or 4000M".into()))};
    number.and_then(|number|number.checked_mul(1024u64.pow(power))).ok_or_else(||CoreError::Validation("Invalid/overflowing memory request".into()))
}

fn pbs_walltime(value:&str) -> CoreResult<String> {
    let has_days=value.contains('-');
    let (days,rest)=match value.split_once('-') {Some((days,rest))=>(days.parse::<u64>().ok(),rest),None=>(Some(0),value)};
    let parts:Vec<_>=rest.split(':').map(str::parse::<u64>).collect();
    let seconds=match parts.as_slice() {
        [Ok(hours)] if has_days=>hours.checked_mul(3600),
        [Ok(hours),Ok(minutes)] if has_days && *minutes<60=>hours.checked_mul(3600).and_then(|v|v.checked_add(minutes*60)),
        [Ok(minutes)] if !has_days=>minutes.checked_mul(60),
        [Ok(minutes),Ok(seconds)] if !has_days && *seconds<60=>minutes.checked_mul(60).and_then(|v|v.checked_add(*seconds)),
        [Ok(hours),Ok(minutes),Ok(seconds)] if *minutes<60 && *seconds<60=>hours.checked_mul(3600).and_then(|v|v.checked_add(minutes*60+seconds)),
        _=>None,
    }.and_then(|seconds|days.and_then(|days|days.checked_mul(86400)).and_then(|days|days.checked_add(seconds)))
        .ok_or_else(||CoreError::Validation("Invalid PBS walltime".into()))?;
    Ok(format!("{:02}:{:02}:{:02}",seconds/3600,(seconds%3600)/60,seconds%60))
}

#[cfg(test)]
mod tests;
