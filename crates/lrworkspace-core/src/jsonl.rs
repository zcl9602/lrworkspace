//! Read JSONL backwards with fixed-size reads and one bounded record buffer.
use std::io::{Read, Seek, SeekFrom};
use crate::{CoreError, CoreResult};
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// The callback returns false when enough valid records have been found.
pub fn visit_reverse_lines(reader: &mut (impl Read + Seek), mut visit: impl FnMut(&[u8]) -> bool) -> CoreResult<()> {
    let mut position = reader.seek(SeekFrom::End(0))?;
    let mut chunk = [0u8; 8192];
    let mut line = Vec::new();
    while position > 0 {
        let count = position.min(chunk.len() as u64) as usize;
        position -= count as u64;
        reader.seek(SeekFrom::Start(position))?;
        reader.read_exact(&mut chunk[..count])?;
        for &byte in chunk[..count].iter().rev() {
            if byte == b'\n' {
                if !line.is_empty() {
                    line.reverse();
                    if !visit(&line) { return Ok(()); }
                    line.clear();
                }
            } else {
                if line.len() == MAX_RECORD_BYTES { return Err(CoreError::Validation("JSONL record exceeds 16 MiB".into())); }
                line.push(byte);
            }
        }
    }
    if !line.is_empty() { line.reverse(); visit(&line); }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reverse_crosses_chunks_handles_crlf_and_stops_early() {
        let input=format!("first\r\n{}\nlast", "x".repeat(20000));
        let mut lines=Vec::new();
        visit_reverse_lines(&mut std::io::Cursor::new(input), |line| { lines.push(line.to_vec()); lines.len()<2 }).unwrap();
        assert_eq!(lines[0],b"last"); assert_eq!(lines[1].len(),20000);
    }
    #[test]
    fn tail_does_not_read_an_oversized_earlier_record() {
        let input=format!("{}\nlatest\n", "x".repeat(MAX_RECORD_BYTES+1));
        let mut count=0;
        visit_reverse_lines(&mut std::io::Cursor::new(input), |line| { assert_eq!(line,b"latest"); count+=1; false }).unwrap();
        assert_eq!(count,1);
    }
}
