//! Measured host availability, not scheduler reservations or idle/free labels.
use lrworkspace_core::{CoreError,CoreResult};

pub const MIN_FREE_CPU_CORES:f64=3.0;
pub const MIN_FREE_MEMORY_PERCENT:u32=20;

// Only execute through the verified login->compute route. Reading /proc and
// sleeping for one short sample never launches a user workload or a test job.
pub const RESOURCE_PROBE:&str=r#"
lrw_cpu_a=$(awk '/^cpu / {for(i=2;i<=9;i++) total+=$i; idle=$5+$6} /^cpu[0-9]+ / {count++} END {printf "%.0f %.0f %d\n",total,idle,count}' /proc/stat) || exit 26
sleep 0.2 || exit 26
lrw_cpu_b=$(awk '/^cpu / {for(i=2;i<=9;i++) total+=$i; idle=$5+$6} /^cpu[0-9]+ / {count++} END {printf "%.0f %.0f %d\n",total,idle,count}' /proc/stat) || exit 26
lrw_memory=$(awk '/^MemTotal:/ {total=$2} /^MemAvailable:/ {available=$2; found=1} /^MemFree:/ {free=$2} END {if(found) printf "%.0f %.0f MemAvailable\n",total,available; else printf "%.0f %.0f MemFree\n",total,free}' /proc/meminfo) || exit 26
printf '%s\n%s\n%s\n' "$lrw_cpu_a" "$lrw_cpu_b" "$lrw_memory" | awk '
NR==1 {a=$1; idle_a=$2; count_a=$3}
NR==2 {b=$1; idle_b=$2; count_b=$3}
NR==3 {mem_total=$1; mem_available=$2; basis=$3}
END {
    delta=b-a; idle_delta=idle_b-idle_a
    if(NR!=3 || delta<=0 || idle_delta<0 || idle_delta>delta || count_a<1 || count_a!=count_b || mem_total<=0 || mem_available<0 || mem_available>mem_total) exit 26
    printf "LRW_RESOURCES %.9f %d %.0f %.0f %s\n",count_b*idle_delta/delta,count_b,mem_available,mem_total,basis
}'
"#;

#[derive(Debug,Clone,serde::Serialize)]
pub struct ResourceSnapshot {
    pub available_cpu_cores:f64,
    pub total_cpu_cores:u32,
    pub available_memory_kib:u64,
    pub total_memory_kib:u64,
    pub available_memory_percent:f64,
    pub memory_basis:String,
    pub cpu_sample_milliseconds:u32,
    pub observed_at:chrono::DateTime<chrono::Utc>,
}

impl ResourceSnapshot {
    pub fn parse(stdout:&str)->CoreResult<Self> {
        let invalid=||CoreError::Validation("Compute CPU/memory measurement is missing or invalid; availability remains unknown".into());
        let line=stdout.lines().rev().find(|line|line.starts_with("LRW_RESOURCES ")).ok_or_else(invalid)?;
        let fields:Vec<_>=line.split_whitespace().collect();
        if fields.len()!=6 {return Err(invalid());}
        let cpu:f64=fields[1].parse().map_err(|_|invalid())?;
        let total_cpu:u32=fields[2].parse().map_err(|_|invalid())?;
        let memory:u64=fields[3].parse().map_err(|_|invalid())?;
        let total_memory:u64=fields[4].parse().map_err(|_|invalid())?;
        if !cpu.is_finite() || cpu<0.0 || total_cpu==0 || cpu>f64::from(total_cpu)
            || total_memory==0 || memory>total_memory || !["MemAvailable","MemFree"].contains(&fields[5]) {return Err(invalid());}
        Ok(Self{available_cpu_cores:cpu,total_cpu_cores:total_cpu,available_memory_kib:memory,total_memory_kib:total_memory,
            available_memory_percent:memory as f64/total_memory as f64*100.0,memory_basis:fields[5].into(),
            cpu_sample_milliseconds:200,observed_at:chrono::Utc::now()})
    }
    pub fn admitted(&self)->bool {
        self.available_cpu_cores>=MIN_FREE_CPU_CORES
            && u128::from(self.available_memory_kib)*100>u128::from(self.total_memory_kib)*u128::from(MIN_FREE_MEMORY_PERCENT)
    }
    pub fn summary(&self)->String {
        format!("measured free CPU={:.3}/{}; available RAM={:.2}% ({}); requires CPU >= {} and RAM > {}%",
            self.available_cpu_cores,self.total_cpu_cores,self.available_memory_percent,self.memory_basis,MIN_FREE_CPU_CORES,MIN_FREE_MEMORY_PERCENT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn availability_thresholds_are_measured_and_memory_is_strictly_above_twenty_percent() {
        for (input,admitted) in [("3 8 201 1000 MemAvailable",true),("3 8 200 1000 MemAvailable",false),
            ("2.999 8 800 1000 MemAvailable",false),("6.4 8 800 1000 MemFree",true)] {
            let snapshot=ResourceSnapshot::parse(&format!("banner\nLRW_RESOURCES {input}\n")).unwrap();
            assert_eq!(snapshot.admitted(),admitted,"{input}");
        }
        for input in ["NaN 8 900 1000 MemAvailable","9 8 900 1000 MemAvailable","3 8 1001 1000 MemAvailable",
            "3 0 900 1000 MemAvailable","3 8 0 0 MemAvailable","3 8 800 1000 guessed","malformed"] {
            assert!(ResourceSnapshot::parse(&format!("LRW_RESOURCES {input}")).is_err(),"{input}");
        }
    }
}
