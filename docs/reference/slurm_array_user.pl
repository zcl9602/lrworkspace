#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long;
use List::Util qw(shuffle);
use Cwd qw(abs_path);
use File::Path qw(make_path remove_tree);

my ($input_command_file,$output_dir,$partition_name,$need_nodes,$ntasks,$each_task_cpu,
    $maxnum,$maxcpu,$jobname,$time,$mem,$submit);

GetOptions(
    'in=s'        => \$input_command_file,
    'outdir=s'    => \$output_dir,
    'partition=s' => \$partition_name,
    'nodes=i'     => \$need_nodes,
    'ntasks=i'    => \$ntasks,
    'cpu=i'       => \$each_task_cpu,
    'maxnum=i'    => \$maxnum,
    'maxcpu=i'    => \$maxcpu,
    'jobname=s'   => \$jobname,
    'time=s'      => \$time,
    'mem=s'       => \$mem,
    'submit!'     => \$submit,
) or die "Error: invalid arguments\n";

$partition_name = "cpu64,cpu128" if !defined($partition_name) || $partition_name eq "";
$need_nodes     = 1              if !defined $need_nodes;
$ntasks         = 1              if !defined $ntasks;
$each_task_cpu  = 1              if !defined $each_task_cpu;
$maxnum         = 1000           if !defined $maxnum;
$maxcpu         = 2000           if !defined $maxcpu;
$jobname        = "job"          if !defined($jobname) || $jobname eq "";
$time           = ""             if !defined $time;
$mem            = ""             if !defined $mem;

if (!defined $input_command_file || !defined $output_dir) {
    print_help();
    exit 1;
}

die "Error: input file not found: $input_command_file\n" if !-e $input_command_file;
die "Error: --nodes must be >= 1\n" if $need_nodes < 1;
die "Error: --ntasks must be >= 1\n" if $ntasks < 1;
die "Error: --cpu must be >= 1\n" if $each_task_cpu < 1;
die "Error: --maxnum must be >= 1\n" if $maxnum < 1;
die "Error: --maxcpu must be >= 1\n" if $maxcpu < 1;
die "Error: invalid --jobname\n" if $jobname !~ /^[A-Za-z0-9_.-]+$/;

make_path($output_dir) if !-d $output_dir;

my $outbasedir = abs_path($output_dir)
    or die "Error: cannot get absolute path: $output_dir\n";

my $input_abs = abs_path($input_command_file)
    or die "Error: cannot get absolute path: $input_command_file\n";

my @commands;
open(my $IN, "<", $input_abs)
    or die "Error: cannot open $input_abs: $!\n";

while (my $line = <$IN>) {
    chomp $line;
    $line =~ s/\r$//;
    next if $line =~ /^\s*$/;
    next if $line =~ /^\s*#/;
    push @commands, $line;
}

close $IN;

@commands = shuffle(@commands);

my $allnum = scalar @commands;
die "Error: no valid commands found\n" if $allnum == 0;

# MaxArraySize = 1001, use array index 1-1000.
my $max_array_tasks = 1000;

# At most 1000 command files / array elements.
my $array_tasks = $allnum;
$array_tasks = $max_array_tasks if $array_tasks > $max_array_tasks;

my $cpu_per_task = $ntasks * $each_task_cpu;
my $parallel_by_cpu = int($maxcpu / $cpu_per_task);

die "Error: --maxcpu is smaller than CPU requirement of one task\n"
    if $parallel_by_cpu < 1;

my $array_parallel = $array_tasks;
$array_parallel = $maxnum if $array_parallel > $maxnum;
$array_parallel = $parallel_by_cpu if $array_parallel > $parallel_by_cpu;

my $array_dir = "$outbasedir/array.0001";
my $cmd_dir   = "$array_dir/cmd";
my $out_dir   = "$array_dir/out";
my $err_dir   = "$array_dir/err";

make_path($array_dir) if !-d $array_dir;

# Recreate these directories so an old run does not leave stale files.
remove_tree($cmd_dir) if -d $cmd_dir;
remove_tree($out_dir) if -d $out_dir;
remove_tree($err_dir) if -d $err_dir;

make_path($cmd_dir);
make_path($out_dir);
make_path($err_dir);

my $base_num = int($allnum / $array_tasks);
my $mod_num  = $allnum % $array_tasks;
my $cmd_index = 0;

my $manifest_file = "$array_dir/manifest.tsv";

open(my $MAN, ">", $manifest_file)
    or die "Error: cannot write $manifest_file: $!\n";

print $MAN join("\t",
    qw(task_id command_start command_end command_count command_file)
), "\n";

for my $task_id (1 .. $array_tasks) {
    my $this_num = $base_num;
    $this_num++ if $task_id <= $mod_num;

    my $start = $cmd_index + 1;
    my $end   = $cmd_index + $this_num;
    my $tag   = sprintf("%04d", $task_id);
    my $cmd_file = "$cmd_dir/cmd.$tag.sh";

    open(my $CMD, ">", $cmd_file)
        or die "Error: cannot write $cmd_file: $!\n";

    print $CMD "#!/usr/bin/env bash\n\n";
    print $CMD "FAILED=0\n";
    print $CMD "TOTAL=$this_num\n\n";
    print $CMD "echo \"Command file : $tag\"\n";
    print $CMD "echo \"Commands     : $start-$end\"\n";
    print $CMD "echo \"Start        : \$(date)\"\n\n";

    for (1 .. $this_num) {
        my $global_index = $cmd_index + 1;
        my $cmd = $commands[$cmd_index];

        print $CMD "echo \"----------------------------------------\"\n";
        print $CMD "echo \"Command index: $global_index / $allnum\"\n";
        print $CMD "(\n";
        print $CMD "$cmd\n";
        print $CMD ")\n";
        print $CMD "status=\$?\n";
        print $CMD "if (( status != 0 )); then\n";
        print $CMD "    echo \"ERROR: command $global_index failed with exit status \$status\" >&2\n";
        print $CMD "    FAILED=\$((FAILED + 1))\n";
        print $CMD "fi\n\n";

        $cmd_index++;
    }

    print $CMD "echo \"----------------------------------------\"\n";
    print $CMD "echo \"End          : \$(date)\"\n";
    print $CMD "echo \"Failed       : \$FAILED / \$TOTAL\"\n";
    print $CMD "if (( FAILED > 0 )); then\n";
    print $CMD "    exit 1\n";
    print $CMD "else\n";
    print $CMD "    exit 0\n";
    print $CMD "fi\n";

    close $CMD;

    chmod 0755, $cmd_file
        or die "Error: chmod failed: $cmd_file\n";

    print $MAN join("\t",
        $task_id,$start,$end,$this_num,$cmd_file
    ), "\n";
}

close $MAN;

die "Error: internal command distribution error: $cmd_index != $allnum\n"
    if $cmd_index != $allnum;

my $slurm_file = "$array_dir/$jobname.array.slurm";
my $quoted_cmd_dir = shell_quote($cmd_dir);

open(my $OUT, ">", $slurm_file)
    or die "Error: cannot write $slurm_file: $!\n";

print $OUT <<"EOF";
#!/usr/bin/env bash
#SBATCH --job-name=$jobname
#SBATCH --partition=$partition_name
#SBATCH --nodes=$need_nodes
#SBATCH --ntasks=$ntasks
#SBATCH --cpus-per-task=$each_task_cpu
#SBATCH --array=1-$array_tasks%$array_parallel
#SBATCH --output=$out_dir/$jobname.%A_%a.out
#SBATCH --error=$err_dir/$jobname.%A_%a.err
EOF

print $OUT "#SBATCH --time=$time\n" if $time ne "";
print $OUT "#SBATCH --mem=$mem\n" if $mem ne "";

print $OUT <<"EOF";

cd "\$SLURM_SUBMIT_DIR" || exit 1

CMD_DIR=$quoted_cmd_dir
printf -v TAG "%04d" "\$SLURM_ARRAY_TASK_ID"
CMD_FILE="\$CMD_DIR/cmd.\$TAG.sh"

if [[ ! -r "\$CMD_FILE" ]]; then
    echo "ERROR: command file not readable: \$CMD_FILE" >&2
    exit 1
fi

echo "Job ID       : \$SLURM_JOB_ID"
echo "Array Job ID : \$SLURM_ARRAY_JOB_ID"
echo "Array Task ID: \$SLURM_ARRAY_TASK_ID"
echo "Node         : \$(hostname)"
echo "Command file : \$CMD_FILE"
echo "Start        : \$(date)"

bash "\$CMD_FILE"
status=\$?

echo "End          : \$(date)"
echo "Exit status  : \$status"

exit "\$status"
EOF

close $OUT;

chmod 0755, $slurm_file
    or die "Error: chmod failed: $slurm_file\n";

print STDERR "Generated:\n";
print STDERR "  Commands       : $allnum\n";
print STDERR "  Array tasks    : $array_tasks\n";
print STDERR "  CPU/task       : $cpu_per_task\n";
print STDERR "  Parallel       : $array_parallel\n";

if ($mod_num > 0) {
    print STDERR "  Commands/task  : $base_num or " . ($base_num + 1) . "\n";
} else {
    print STDERR "  Commands/task  : $base_num\n";
}

print STDERR "  Array dir      : $array_dir\n";
print STDERR "  Command dir    : $cmd_dir\n";
print STDERR "  Out dir        : $out_dir\n";
print STDERR "  Err dir        : $err_dir\n";
print STDERR "  Manifest       : $manifest_file\n";
print STDERR "  Slurm          : $slurm_file\n";
print STDERR "  Array          : 1-$array_tasks%$array_parallel\n";

if ($submit) {
    system("sbatch", $slurm_file) == 0
        or die "Error: sbatch failed: $slurm_file\n";
} else {
    print STDERR "Submit: sbatch $slurm_file\n";
}

sub shell_quote {
    my ($s) = @_;
    $s =~ s/'/'"'"'/g;
    return "'$s'";
}

sub print_help {
    print STDERR <<'EOF';
Usage: slurm_array.pl --in commands.sh --outdir slurm_dir [options]

Options:
  --jobname NAME          default: job
  --partition PARTITION   default: cpu64,cpu128
  --nodes N               default: 1
  --ntasks N              default: 1
  --cpu N                 default: 1
  --maxnum N              max parallel array tasks, default: 1000
  --maxcpu N              max parallel CPUs, default: 2000
  --time TIME             e.g. 24:00:00
  --mem MEM               e.g. 4G
  --submit                 submit immediately

Example:
  slurm_array.pl --in commands.sh --outdir JOBS --jobname test --cpu 1 --maxnum 500 --submit
EOF
}