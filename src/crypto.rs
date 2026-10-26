//! Every hash, MAC, key derivation, random byte, constant-time comparison, sealing and signature the
//! lake uses, in one place, so each form gives its own (aws-lc-rs on a node; RustCrypto on a page).
//! Callers never name aws-lc-rs.

// A node's bodies: aws-lc-rs, which a page can't build.
#[cfg(not(target_arch = "wasm32"))]
mod native {
    use anyhow::{anyhow, Result};
    use std::num::NonZeroU32;

    /// Fills `buf` with random bytes.
    pub fn random(buf: &mut [u8]) -> Result<()> {
        aws_lc_rs::rand::fill(buf).map_err(|_| anyhow!("no randomness"))
    }

    /// Are `a` and `b` the same bytes? Compared in constant time.
    pub fn same(a: &[u8], b: &[u8]) -> bool {
        aws_lc_rs::constant_time::verify_slices_are_equal(a, b).is_ok()
    }

    /// SHA-1 of `parts` one after another: git's blob ids, and for nothing else.
    pub fn sha1(parts: &[&[u8]]) -> [u8; 20] {
        let mut ctx = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA1_FOR_LEGACY_USE_ONLY);
        for part in parts {
            ctx.update(part);
        }
        ctx.finish().as_ref().try_into().expect("20 bytes")
    }

    /// SHA-256 of `data`.
    pub fn sha256(data: &[u8]) -> [u8; 32] {
        aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, data).as_ref().try_into().expect("32 bytes")
    }

    /// SHA-512 of `data`.
    pub fn sha512(data: &[u8]) -> [u8; 64] {
        aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA512, data).as_ref().try_into().expect("64 bytes")
    }

    /// HMAC-SHA-256 of `data` under `key`.
    pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
        aws_lc_rs::hmac::sign(&aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, key), data).as_ref().try_into().expect("32 bytes")
    }

    /// HMAC-SHA-512 of `data` under `key`.
    pub fn hmac_sha512(key: &[u8], data: &[u8]) -> [u8; 64] {
        aws_lc_rs::hmac::sign(&aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA512, key), data).as_ref().try_into().expect("64 bytes")
    }

    /// Is `tag` the HMAC-SHA-256 of `data` under `key`? Compared in constant time.
    pub fn verify_hmac_sha256(key: &[u8], data: &[u8], tag: &[u8]) -> bool {
        same(&hmac_sha256(key, data), tag)
    }

    /// PBKDF2-HMAC-SHA-256: `out` filled with the key derived from `secret` and `salt` over `iterations` rounds.
    pub fn pbkdf2_sha256(iterations: NonZeroU32, salt: &[u8], secret: &[u8], out: &mut [u8]) {
        aws_lc_rs::pbkdf2::derive(aws_lc_rs::pbkdf2::PBKDF2_HMAC_SHA256, iterations, salt, secret, out)
    }

    /// PBKDF2-HMAC-SHA-512: `out` filled with the key derived from `secret` and `salt` over `iterations` rounds.
    pub fn pbkdf2_sha512(iterations: NonZeroU32, salt: &[u8], secret: &[u8], out: &mut [u8]) {
        aws_lc_rs::pbkdf2::derive(aws_lc_rs::pbkdf2::PBKDF2_HMAC_SHA512, iterations, salt, secret, out)
    }

    /// `data` sealed in place with AES-256-GCM under `key` and `nonce` (no associated data): its tag is appended.
    pub fn seal(key: &[u8; 32], nonce: [u8; 12], data: &mut Vec<u8>) -> Result<()> {
        aws_lc_rs::aead::LessSafeKey::new(aws_lc_rs::aead::UnboundKey::new(&aws_lc_rs::aead::AES_256_GCM, key).map_err(|_| anyhow!("key"))?)
            .seal_in_place_append_tag(aws_lc_rs::aead::Nonce::assume_unique_for_key(nonce), aws_lc_rs::aead::Aad::empty(), data)
            .map_err(|_| anyhow!("sealing failed"))
    }

    /// `data` (sealed by `seal`, tag last) opened in place under `key` and `nonce`: the plain text, or an error if it doesn't open.
    pub fn open<'a>(key: &[u8; 32], nonce: [u8; 12], data: &'a mut [u8]) -> Result<&'a [u8]> {
        let k = aws_lc_rs::aead::LessSafeKey::new(aws_lc_rs::aead::UnboundKey::new(&aws_lc_rs::aead::AES_256_GCM, key).map_err(|_| anyhow!("key"))?);
        let plain = k.open_in_place(aws_lc_rs::aead::Nonce::assume_unique_for_key(nonce), aws_lc_rs::aead::Aad::empty(), data).map_err(|_| anyhow!("opening failed"))?;
        Ok(plain)
    }

    /// Is `signature` the RS256 signature (RSASSA-PKCS1-v1_5, SHA-256) of `message` by the RSA key (`n`, `e`)? A 2048- to 8192-bit key.
    pub fn verify_rs256(n: &[u8], e: &[u8], message: &[u8], signature: &[u8]) -> bool {
        verify_rsa(&aws_lc_rs::signature::RSA_PKCS1_2048_8192_SHA256, n, e, message, signature)
    }

    /// Is `signature` the RS384 signature (RSASSA-PKCS1-v1_5, SHA-384) of `message` by the RSA key (`n`, `e`)? A 2048- to 8192-bit key.
    pub fn verify_rs384(n: &[u8], e: &[u8], message: &[u8], signature: &[u8]) -> bool {
        verify_rsa(&aws_lc_rs::signature::RSA_PKCS1_2048_8192_SHA384, n, e, message, signature)
    }

    /// Is `signature` the RS512 signature (RSASSA-PKCS1-v1_5, SHA-512) of `message` by the RSA key (`n`, `e`)? A 2048- to 8192-bit key.
    pub fn verify_rs512(n: &[u8], e: &[u8], message: &[u8], signature: &[u8]) -> bool {
        verify_rsa(&aws_lc_rs::signature::RSA_PKCS1_2048_8192_SHA512, n, e, message, signature)
    }

    /// Is `signature` the PS256 signature (RSASSA-PSS, SHA-256) of `message` by the RSA key (`n`, `e`)? A 2048- to 8192-bit key.
    pub fn verify_ps256(n: &[u8], e: &[u8], message: &[u8], signature: &[u8]) -> bool {
        verify_rsa(&aws_lc_rs::signature::RSA_PSS_2048_8192_SHA256, n, e, message, signature)
    }

    fn verify_rsa(params: &'static aws_lc_rs::signature::RsaParameters, n: &[u8], e: &[u8], message: &[u8], signature: &[u8]) -> bool {
        aws_lc_rs::signature::RsaPublicKeyComponents { n, e }.verify(params, message, signature).is_ok()
    }

    /// Is `signature` the ES256 signature (ECDSA, P-256, SHA-256, fixed r‖s) of `message` by the uncompressed `point`?
    pub fn verify_es256(point: &[u8], message: &[u8], signature: &[u8]) -> bool {
        aws_lc_rs::signature::UnparsedPublicKey::new(&aws_lc_rs::signature::ECDSA_P256_SHA256_FIXED, point).verify(message, signature).is_ok()
    }

    /// Is `signature` the ES384 signature (ECDSA, P-384, SHA-384, fixed r‖s) of `message` by the uncompressed `point`?
    pub fn verify_es384(point: &[u8], message: &[u8], signature: &[u8]) -> bool {
        aws_lc_rs::signature::UnparsedPublicKey::new(&aws_lc_rs::signature::ECDSA_P384_SHA384_FIXED, point).verify(message, signature).is_ok()
    }
}

// A page's bodies: RustCrypto and getrandom, which build for wasm32. A native test compares the ones it can with the node's.
#[cfg(any(target_arch = "wasm32", test))]
#[cfg_attr(test, allow(dead_code))] // (the rest only a page calls)
mod page {
    use anyhow::{anyhow, Result};
    use hmac::{Hmac, KeyInit, Mac};
    use sha1::{Digest, Sha1};
    use sha2::{Sha256, Sha512};
    use std::num::NonZeroU32;
    use subtle::ConstantTimeEq;

    /// Fills `buf` with random bytes.
    pub fn random(buf: &mut [u8]) -> Result<()> {
        getrandom::fill(buf).map_err(|_| anyhow!("no randomness"))
    }

    /// Are `a` and `b` the same bytes? Compared in constant time.
    pub fn same(a: &[u8], b: &[u8]) -> bool {
        bool::from(a.ct_eq(b))
    }

    /// SHA-1 of `parts` one after another: git's blob ids, and for nothing else.
    pub fn sha1(parts: &[&[u8]]) -> [u8; 20] {
        let mut h = Sha1::new();
        for part in parts {
            h.update(part);
        }
        h.finalize().into()
    }

    /// SHA-256 of `data`.
    pub fn sha256(data: &[u8]) -> [u8; 32] {
        Sha256::digest(data).into()
    }

    /// SHA-512 of `data`.
    pub fn sha512(data: &[u8]) -> [u8; 64] {
        Sha512::digest(data).into()
    }

    /// HMAC-SHA-256 of `data` under `key`.
    pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
        let mut m = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes a key of any length");
        m.update(data);
        m.finalize().into_bytes().into()
    }

    /// HMAC-SHA-512 of `data` under `key`.
    pub fn hmac_sha512(key: &[u8], data: &[u8]) -> [u8; 64] {
        let mut m = Hmac::<Sha512>::new_from_slice(key).expect("HMAC takes a key of any length");
        m.update(data);
        m.finalize().into_bytes().into()
    }

    /// Is `tag` the HMAC-SHA-256 of `data` under `key`? Compared in constant time.
    pub fn verify_hmac_sha256(key: &[u8], data: &[u8], tag: &[u8]) -> bool {
        let mut m = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes a key of any length");
        m.update(data);
        m.verify_slice(tag).is_ok()
    }

    /// PBKDF2-HMAC-SHA-256: `out` filled with the key derived from `secret` and `salt` over `iterations` rounds.
    pub fn pbkdf2_sha256(iterations: NonZeroU32, salt: &[u8], secret: &[u8], out: &mut [u8]) {
        pbkdf2::<Hmac<Sha256>>(iterations, salt, secret, out)
    }

    /// PBKDF2-HMAC-SHA-512: `out` filled with the key derived from `secret` and `salt` over `iterations` rounds.
    pub fn pbkdf2_sha512(iterations: NonZeroU32, salt: &[u8], secret: &[u8], out: &mut [u8]) {
        pbkdf2::<Hmac<Sha512>>(iterations, salt, secret, out)
    }

    /// RFC 8018 §5.2 over the HMAC `M`: block `i` of `out` is U_1 ^ … ^ U_c, with U_1 = HMAC(secret, salt ‖ i).
    fn pbkdf2<M: Mac + KeyInit + Clone>(iterations: NonZeroU32, salt: &[u8], secret: &[u8], out: &mut [u8]) {
        let prf = M::new_from_slice(secret).expect("HMAC takes a key of any length");
        for (i, block) in out.chunks_mut(M::output_size()).enumerate() {
            let mut m = prf.clone();
            m.update(salt);
            m.update(&(i as u32 + 1).to_be_bytes());
            let mut u = m.finalize().into_bytes();
            let mut t = u.clone();
            for _ in 1..iterations.get() {
                let mut m = prf.clone();
                m.update(&u);
                u = m.finalize().into_bytes();
                t.iter_mut().zip(u.iter()).for_each(|(t, u)| *t ^= u);
            }
            block.copy_from_slice(&t[..block.len()]);
        }
    }

    /// Refused on a page: a secret is sealed with a key only a node holds.
    pub fn seal(_key: &[u8; 32], _nonce: [u8; 12], _data: &mut Vec<u8>) -> Result<()> {
        Err(crate::codes::coded("0A000", "a page seals no secrets: a secret is sealed with a key only a node holds"))
    }

    /// Refused on a page: a sealed secret is opened with a key only a node holds.
    pub fn open<'a>(_key: &[u8; 32], _nonce: [u8; 12], _data: &'a mut [u8]) -> Result<&'a [u8]> {
        Err(crate::codes::coded("0A000", "a page opens no sealed secrets: a sealed secret opens with a key only a node holds"))
    }

    /// A page verifies no RSA or ECDSA signature: sign-in is a node's door.
    pub fn verify_rs256(_n: &[u8], _e: &[u8], _message: &[u8], _signature: &[u8]) -> bool {
        false
    }

    /// A page verifies no RSA or ECDSA signature: sign-in is a node's door.
    pub fn verify_rs384(_n: &[u8], _e: &[u8], _message: &[u8], _signature: &[u8]) -> bool {
        false
    }

    /// A page verifies no RSA or ECDSA signature: sign-in is a node's door.
    pub fn verify_rs512(_n: &[u8], _e: &[u8], _message: &[u8], _signature: &[u8]) -> bool {
        false
    }

    /// A page verifies no RSA or ECDSA signature: sign-in is a node's door.
    pub fn verify_ps256(_n: &[u8], _e: &[u8], _message: &[u8], _signature: &[u8]) -> bool {
        false
    }

    /// A page verifies no RSA or ECDSA signature: sign-in is a node's door.
    pub fn verify_es256(_point: &[u8], _message: &[u8], _signature: &[u8]) -> bool {
        false
    }

    /// A page verifies no RSA or ECDSA signature: sign-in is a node's door.
    pub fn verify_es384(_point: &[u8], _message: &[u8], _signature: &[u8]) -> bool {
        false
    }
}

// Each page body is re-exported only on a page; a node keeps aws-lc-rs's.
#[cfg(not(target_arch = "wasm32"))]
pub use native::*;
#[cfg(target_arch = "wasm32")]
pub use page::*;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::{native, page};
    use std::num::NonZeroU32;

    /// `s` as bytes, two hex digits each.
    fn hex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex")).collect()
    }

    #[test]
    fn page_digests_macs_and_compare_match_the_node() {
        let datas: [&[u8]; 5] = [b"", b"abc", &[7; 65], &[1; 127], &[0xa5; 300]];
        let keys: [&[u8]; 4] = [b"", b"key", &[9; 100], &[3; 200]];
        for data in datas {
            assert_eq!(page::sha1(&[&data[..data.len() / 2], &data[data.len() / 2..]]), native::sha1(&[data]));
            assert_eq!(page::sha256(data), native::sha256(data));
            assert_eq!(page::sha512(data), native::sha512(data));
            for key in keys {
                let tag = native::hmac_sha256(key, data);
                let mut bad = tag;
                bad[31] ^= 1;
                assert_eq!(page::hmac_sha256(key, data), tag);
                assert_eq!(page::hmac_sha512(key, data), native::hmac_sha512(key, data));
                for (t, want) in [(&tag[..], true), (&bad[..], false), (&tag[..31], false)] {
                    assert_eq!(page::verify_hmac_sha256(key, data, t), want);
                    assert_eq!(native::verify_hmac_sha256(key, data, t), want);
                }
            }
        }
        let pairs: [(&[u8], &[u8]); 4] = [(b"", b""), (b"abc", b"abc"), (b"abc", b"abd"), (b"abc", b"ab")];
        for (a, b) in pairs {
            assert_eq!(page::same(a, b), native::same(a, b));
        }
    }

    #[test]
    fn page_pbkdf2_matches_the_node_and_rfc_7914() {
        // RFC 7914 §11's PBKDF2-HMAC-SHA-256 vector: P = "passwd", S = "salt", c = 1, dkLen = 64.
        let mut out = [0u8; 64];
        page::pbkdf2_sha256(NonZeroU32::MIN, b"salt", b"passwd", &mut out);
        assert_eq!(out.to_vec(), hex("55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc49ca9cccf179b645991664b39d77ef317c71b845b1e30bd509112041d3a19783"));
        let cases: [(&[u8], &[u8], usize); 3] = [(b"", b"", 20), (b"passwd", b"salt", 33), (b"key", &[5; 70], 100)];
        for c in [1, 2, 1000] {
            let iterations = NonZeroU32::new(c).expect("nonzero");
            for (secret, salt, n) in cases {
                let (mut a, mut b) = (vec![0u8; n], vec![0u8; n]);
                page::pbkdf2_sha256(iterations, salt, secret, &mut a);
                native::pbkdf2_sha256(iterations, salt, secret, &mut b);
                assert_eq!(a, b);
                page::pbkdf2_sha512(iterations, salt, secret, &mut a);
                native::pbkdf2_sha512(iterations, salt, secret, &mut b);
                assert_eq!(a, b);
            }
        }
    }
}
