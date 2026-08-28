//! Shared admission policy for backend transport endpoints.

use std::net::IpAddr;

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encode bytes with the RFC 4648 standard base64 alphabet and padding.
pub fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(BASE64_ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(BASE64_ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            BASE64_ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            BASE64_ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Decode RFC 4648 standard base64, rejecting invalid padding or alphabet.
pub fn base64_decode(value: &str) -> Option<Vec<u8>> {
    fn sextet(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    let bytes: Vec<u8> = value
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect();
    let unpadded: Vec<u8> = bytes
        .iter()
        .copied()
        .take_while(|byte| *byte != b'=')
        .collect();
    let padding = bytes.len().saturating_sub(unpadded.len());
    if bytes.len() % 4 != 0
        || padding > 2
        || bytes[unpadded.len()..].iter().any(|byte| *byte != b'=')
        || (padding == 1 && unpadded.len() % 4 != 3)
        || (padding == 2 && unpadded.len() % 4 != 2)
    {
        return None;
    }
    let mut out = Vec::with_capacity(unpadded.len() * 3 / 4);
    for chunk in unpadded.chunks(4) {
        let mut n = 0_u32;
        for (index, byte) in chunk.iter().enumerate() {
            n |= u32::from(sextet(*byte)?) << (18 - 6 * index);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

/// Admit encrypted remote backends and the explicit local-development
/// plaintext exception. This must run before credentials are attached or
/// connection retries begin.
pub fn validate_backend_url(url: &str) -> Result<(), String> {
    if url
        .split_once("://")
        .map(|(_, remainder)| {
            remainder
                .split(['/', '?', '#'])
                .next()
                .is_some_and(|authority| authority.contains('@'))
        })
        .unwrap_or(false)
    {
        return Err("backend URL must not contain userinfo".into());
    }
    let endpoint = tonic::transport::Endpoint::from_shared(url.to_owned())
        .map_err(|error| format!("invalid backend URL: {error}"))?;
    let uri = endpoint.uri();
    match uri.scheme_str() {
        Some("https") => Ok(()),
        Some("http") => {
            let host = uri
                .host()
                .ok_or_else(|| "plaintext backend URL has no host".to_string())?;
            let literal_host = host
                .strip_prefix('[')
                .and_then(|host| host.strip_suffix(']'))
                .unwrap_or(host);
            if host.eq_ignore_ascii_case("localhost")
                || literal_host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
            {
                Ok(())
            } else {
                Err(
                    "plaintext backend URL is allowed only for literal loopback or localhost"
                        .into(),
                )
            }
        }
        _ => Err("backend URL must use https (http is loopback-only)".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::{base64_decode, base64_encode, validate_backend_url};

    #[test]
    fn base64_standard_vectors_and_padding_are_canonical() {
        for (plain, encoded) in [
            (b"".as_slice(), ""),
            (b"f".as_slice(), "Zg=="),
            (b"fo".as_slice(), "Zm8="),
            (b"foo".as_slice(), "Zm9v"),
            (b"foobar".as_slice(), "Zm9vYmFy"),
        ] {
            assert_eq!(base64_encode(plain), encoded);
            assert_eq!(base64_decode(encoded).as_deref(), Some(plain));
        }
        for invalid in ["!!!", "Zg=", "Z===", "Zg=a"] {
            assert!(base64_decode(invalid).is_none(), "{invalid}");
        }
    }

    #[test]
    fn encrypted_remote_and_plaintext_loopback_are_admitted() {
        for url in [
            "https://backend.example:50051",
            "http://localhost:50051",
            "http://127.0.0.1:50051",
            "http://[::1]:50051",
        ] {
            assert!(validate_backend_url(url).is_ok(), "{url}");
        }
    }

    #[test]
    fn plaintext_remote_and_lookalike_hosts_are_rejected() {
        for url in [
            "http://backend.example:50051",
            "http://10.0.0.1:50051",
            "http://localhost.example:50051",
        ] {
            assert!(validate_backend_url(url).is_err(), "{url}");
        }
    }

    #[test]
    fn userinfo_is_rejected_without_reflecting_credentials() {
        for url in [
            "https://sentinel-user:sentinel-password@backend.example:50051",
            "http://sentinel-user:sentinel-password@localhost:50051",
        ] {
            let error = validate_backend_url(url).unwrap_err();
            assert_eq!(error, "backend URL must not contain userinfo");
            assert!(!error.contains("sentinel-user"));
            assert!(!error.contains("sentinel-password"));
        }
    }
}
