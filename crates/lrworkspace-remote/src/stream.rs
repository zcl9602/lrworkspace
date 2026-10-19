//! Output retention and frame parsing stay bounded independently of output size.
#[cfg(any(windows,test))]
use lrworkspace_core::{CoreError, CoreResult};

pub(crate) struct OutputBuffer {
    bytes: Vec<u8>,
    limit: usize,
    pub truncated: bool,
}
impl OutputBuffer {
    pub fn new(limit: usize) -> Self { Self { bytes: Vec::new(), limit, truncated: false } }
    pub fn push(&mut self, bytes: &[u8]) {
        let count = bytes.len().min(self.limit.saturating_sub(self.bytes.len()));
        self.bytes.extend_from_slice(&bytes[..count]);
        self.truncated |= count != bytes.len();
    }
    pub fn into_bytes(self) -> (Vec<u8>, bool) { (self.bytes, self.truncated) }
    pub fn into_text(self) -> (String, bool) {
        let limit = self.limit;
        let mut truncated = self.truncated;
        let mut text = String::from_utf8(self.bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
        if text.len() > limit {
            let mut end = limit;
            while !text.is_char_boundary(end) { end -= 1; }
            text.truncate(end);
            truncated = true;
        }
        (text, truncated)
    }
}

/// Scan only a new chunk plus a marker-sized suffix, never the retained output.
#[cfg(any(windows,test))]
pub(crate) struct FrameReader {
    begin: Vec<u8>, end: Vec<u8>, pending: Vec<u8>, started: bool,
    ending: bool, with_code: bool, pub done: bool, pub code: Option<i32>,
    output: OutputBuffer, banner: OutputBuffer,
}
#[cfg(any(windows,test))]
impl FrameReader {
    pub fn new(token: &str, with_code: bool, limit: usize) -> Self {
        Self { begin: format!("\x1eLRW_BEGIN:{token}\x1f").into_bytes(),
            end: if with_code { format!("\x1eLRW_END:{token}:") } else { format!("\x1eLRW_END:{token}\x1f") }.into_bytes(),
            pending: Vec::new(), started: false, ending: false, with_code,
            done: false, code: None, output: OutputBuffer::new(limit), banner: OutputBuffer::new(4096) }
    }
    pub fn push(&mut self, bytes: &[u8]) -> CoreResult<()> {
        if self.done { return Ok(()); }
        self.pending.extend_from_slice(bytes);
        if !self.started {
            if let Some(index) = find(&self.pending, &self.begin) {
                self.banner.push(&self.pending[..index]);
                self.pending.drain(..index + self.begin.len()); self.started = true;
            } else {
                let discard = self.pending.len().saturating_sub(self.begin.len()-1);
                self.banner.push(&self.pending[..discard]);
                self.pending.drain(..discard); return Ok(());
            }
        }
        if !self.ending {
            if let Some(index) = find(&self.pending, &self.end) {
                self.output.push(&self.pending[..index]);
                self.pending.drain(..index+self.end.len());
                self.ending = true;
                if !self.with_code { self.done = true; self.pending.clear(); return Ok(()); }
            } else {
                let consume = self.pending.len().saturating_sub(self.end.len()-1);
                self.output.push(&self.pending[..consume]);
                self.pending.drain(..consume); return Ok(());
            }
        }
        if let Some(end) = self.pending.iter().position(|b| *b == 0x1f) {
            self.code = std::str::from_utf8(&self.pending[..end]).ok().and_then(|s| s.parse().ok());
            if self.code.is_none() { return Err(CoreError::Connection("Invalid SSH frame exit code".into())); }
            self.done = true; self.pending.clear();
        } else if self.pending.len() > 12 {
            return Err(CoreError::Connection("Unterminated SSH frame exit code".into()));
        }
        Ok(())
    }
    pub fn diagnostic(&self)->String {
        let bytes=if self.started {&self.output.bytes}else{&self.banner.bytes};
        let mut text=String::from_utf8_lossy(&bytes[..bytes.len().min(2048)]).into_owned();
        if !self.started {text.push_str(&String::from_utf8_lossy(&self.pending[..self.pending.len().min(2048)]));}
        text
    }
    pub fn into_text(self) -> (String, bool) { self.output.into_text() }
}
#[cfg(any(windows,test))]
fn find(bytes: &[u8], marker: &[u8]) -> Option<usize> { bytes.windows(marker.len()).position(|part| part == marker) }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_marker_split_and_overflow_preserve_exit_code() {
        for size in 1..100 {
            let mut frame = FrameReader::new("token", true, 7);
            let text = "banner\x1eLRW_BEGIN:token\x1fabcdefghijk\x1eLRW_END:token:23\x1ftrailing";
            for chunk in text.as_bytes().chunks(size) { frame.push(chunk).unwrap(); }
            assert!(frame.done); assert_eq!(frame.code, Some(23));
            assert_eq!(frame.into_text(), ("abcdefg".into(), true));
        }
    }
    #[test]
    fn stderr_exact_limit_and_banner_keep_only_marker_suffix() {
        let mut frame = FrameReader::new("t", false, 3);
        for _ in 0..100 { frame.push(&[b'x'; 8192]).unwrap(); assert!(frame.pending.len() < 64); }
        frame.push(b"\x1eLRW_BEGIN:t\x1fabc\x1eLRW_END:t\x1f").unwrap();
        assert!(frame.done); assert_eq!(frame.into_text(), ("abc".into(), false));
        let mut output = OutputBuffer::new(4); output.push(&[255; 4]);
        let (text, truncated) = output.into_text(); assert!(text.len() <= 4 && truncated);
    }
}
