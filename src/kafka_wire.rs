//! The Kafka protocol's wire: the reader of a request or response (`Rd`), the writers of its strings and varints,
//! the CRC its record batches carry and their decompression. Both ends use it: the Kafka door (`kafka.rs`), a broker
//! to Kafka's clients, and the Kafka client (`kafka_client.rs`), which reads other clusters' topics as tables.
use anyhow::{anyhow, bail, ensure, Result};
use bytes::{BufMut, Bytes};
use std::io::Read;

/// The most a produce request's record batches may hold decompressed, and one batch a broker sends:
/// a small compressed batch could otherwise ask for gigabytes (S17).
pub(crate) const MOST: u64 = 256 << 20;

/// `data` decompressed, refused if that is over `most` bytes (nothing past it is read).
pub(crate) fn decompress(codec: i16, data: &[u8], most: u64) -> Result<Vec<u8>> {
    let mut out = vec![];
    match codec {
        0 => out.extend_from_slice(data),
        1 => drop(flate2::read::GzDecoder::new(data).take(most + 1).read_to_end(&mut out)?),
        2 => out = unsnappy(data, most)?,
        3 => drop(lz4_flex::frame::FrameDecoder::new(data).take(most + 1).read_to_end(&mut out)?),
        4 => drop(zstd::stream::read::Decoder::new(data)?.take(most + 1).read_to_end(&mut out)?),
        c => bail!("compression codec {c}"),
    }
    ensure!(out.len() as u64 <= most, "record batches over {} MB decompressed", MOST >> 20);
    Ok(out)
}

/// Snappy: raw, or as Java's client frames it (xerial: a header, then length-prefixed blocks). The
/// lengths are read from each block's header, so nothing is allocated past `most`.
fn unsnappy(data: &[u8], most: u64) -> Result<Vec<u8>> {
    let mut snappy = snap::raw::Decoder::new();
    let too_big = || anyhow!("record batches over {} MB decompressed", MOST >> 20);
    if !data.starts_with(b"\x82SNAPPY\x00") {
        ensure!(snap::raw::decompress_len(data)? as u64 <= most, too_big());
        return Ok(snappy.decompress_vec(data)?);
    }
    let (mut out, mut at) = (vec![], 16);
    while at + 4 <= data.len() {
        let n = u32::from_be_bytes(data[at..at + 4].try_into()?) as usize;
        ensure!(at + 4 + n <= data.len(), "truncated snappy block");
        let block = &data[at + 4..at + 4 + n];
        ensure!(out.len() as u64 + snap::raw::decompress_len(block)? as u64 <= most, too_big());
        out.extend(snappy.decompress_vec(block)?);
        at += 4 + n;
    }
    Ok(out)
}

// ---------------------------------------------------------------- encoding

pub(crate) struct Rd {
    pub(crate) b: Bytes,
    pub(crate) at: usize,
}

impl Rd {
    pub(crate) fn take(&mut self, n: usize) -> Result<&[u8]> {
        ensure!(self.at + n <= self.b.len(), "request ends early");
        self.at += n;
        Ok(&self.b[self.at - n..self.at])
    }
    pub(crate) fn i8(&mut self) -> Result<i8> { Ok(self.take(1)?[0] as i8) }
    pub(crate) fn i16(&mut self) -> Result<i16> { Ok(i16::from_be_bytes(self.take(2)?.try_into()?)) }
    pub(crate) fn i32(&mut self) -> Result<i32> { Ok(i32::from_be_bytes(self.take(4)?.try_into()?)) }
    pub(crate) fn i64(&mut self) -> Result<i64> { Ok(i64::from_be_bytes(self.take(8)?.try_into()?)) }
    /// An array's length (a null array reads as empty).
    pub(crate) fn len(&mut self) -> Result<usize> { Ok(self.i32()?.max(0) as usize) }
    pub(crate) fn nstr(&mut self) -> Result<Option<String>> {
        let n = self.i16()?;
        Ok(if n < 0 { None } else { Some(String::from_utf8(self.take(n as usize)?.to_vec())?) })
    }
    pub(crate) fn str(&mut self) -> Result<String> { Ok(self.nstr()?.unwrap_or_default()) }
    pub(crate) fn nbytes(&mut self) -> Result<Option<Bytes>> {
        let n = self.i32()?;
        Ok(if n < 0 { None } else { Some(self.slice(n as usize)?) })
    }
    pub(crate) fn slice(&mut self, n: usize) -> Result<Bytes> {
        self.take(n)?;
        Ok(self.b.slice(self.at - n..self.at))
    }
    pub(crate) fn uvarint(&mut self) -> Result<u64> {
        let (mut v, mut shift) = (0u64, 0);
        loop {
            let b = self.take(1)?[0];
            v |= ((b & 0x7f) as u64) << shift;
            if b < 0x80 {
                return Ok(v);
            }
            shift += 7;
            ensure!(shift < 64, "bad varint");
        }
    }
    pub(crate) fn varint(&mut self) -> Result<i64> {
        let v = self.uvarint()?;
        Ok((v >> 1) as i64 ^ -((v & 1) as i64))
    }
    /// Varint-length bytes inside a record (-1 = null).
    pub(crate) fn vbytes(&mut self) -> Result<Option<Bytes>> {
        let n = self.varint()?;
        Ok(if n < 0 { None } else { Some(self.slice(n as usize)?) })
    }
    /// Tagged fields (flexible versions): skipped.
    pub(crate) fn tags(&mut self) -> Result<()> {
        for _ in 0..self.uvarint()? {
            self.uvarint()?;
            let n = self.uvarint()? as usize;
            self.take(n)?;
        }
        Ok(())
    }
}

pub(crate) fn put_str(w: &mut Vec<u8>, s: &str) {
    w.put_i16(s.len() as i16);
    w.put_slice(s.as_bytes());
}

pub(crate) fn put_nstr(w: &mut Vec<u8>, s: Option<&str>) {
    match s {
        Some(s) => put_str(w, s),
        None => w.put_i16(-1),
    }
}

/// An array length: i32, or in flexible versions a varint of length + 1.
pub(crate) fn put_len(w: &mut Vec<u8>, n: usize, compact: bool) {
    match compact {
        true => put_uvarint(w, n as u64 + 1),
        false => w.put_i32(n as i32),
    }
}

pub(crate) fn put_uvarint(w: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        w.put_u8(v as u8 | 0x80);
        v >>= 7;
    }
    w.put_u8(v as u8);
}

pub(crate) fn put_varint(w: &mut Vec<u8>, v: i64) { put_uvarint(w, ((v << 1) ^ (v >> 63)) as u64) }

pub(crate) fn crc32c(data: &[u8]) -> u32 { crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, data) as u32 }
