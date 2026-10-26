//! Every hash, MAC, key derivation, random byte, constant-time comparison, sealing and signature the
//! lake uses, in one place, so each form gives its own (aws-lc-rs on a node; a page's comes later).
//! Callers never name aws-lc-rs.
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
