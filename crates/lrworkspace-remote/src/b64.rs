//! Minimal, dependency-free base64 codec used for binary-safe transfer of
//! file contents over the SSH exec channel (fallback when SFTP is unavailable).

const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encode raw bytes to a standard base64 string (no line wrapping).
pub fn encode(data: &[u8]) -> String {
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk.first().copied().unwrap_or(0);
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);

        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);

        result.push(TABLE[((n >> 18) & 0x3F) as usize] as char);
        result.push(TABLE[((n >> 12) & 0x3F) as usize] as char);
        result.push(if chunk.len() > 1 {
            TABLE[((n >> 6) & 0x3F) as usize] as char
        } else {
            '='
        });
        result.push(if chunk.len() > 2 {
            TABLE[(n & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    result
}

/// Decode a base64 string to raw bytes.
pub fn decode(input: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    let mut acc = 0u32;
    let mut bits = 0u8;
    for &byte in input.as_bytes() {
        let val = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            b'\n' | b'\r' | b' ' | b'\t' | 0x0b | 0x0c => continue,
            other => return Err(format!("invalid base64 byte: 0x{other:02x}")),
        };
        acc = (acc << 6) | u32::from(val);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_known_vectors() {
        assert_eq!(encode(b""), "");
        assert_eq!(encode(b"f"), "Zg==");
        assert_eq!(encode(b"foo"), "Zm9v");
    }

    #[test]
    fn roundtrip_binary_payload() {
        let data: Vec<u8> = (0u16..=255).map(|b| b as u8).collect();
        assert_eq!(decode(&encode(&data)).unwrap(), data);
    }
}
