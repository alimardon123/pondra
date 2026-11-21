//! INET (the internal name IPADDRESS; L6's words, 11-14): one IPv4 or IPv6 address, as Postgres's `inet` and DuckDB's
//! `INET` name it. It is held in 17 bytes (FixedSizeBinary(17)): its family (4 or 6), then its 16 bytes, an IPv4 address
//! as its IPv4-mapped IPv6 one (`::ffff:a.b.c.d`). So values compare, sort, group and join by their bytes as Postgres's
//! and DuckDB's do: every IPv4 address before every IPv6 one, and `::ffff:1.2.3.4` apart from `1.2.3.4` (L1, 11-21). No
//! netmask is kept (CIDR comes later): a host's full mask is taken (`10.0.0.1/32`, `::1/128`) and a network's refused. A
//! value is shown as Postgres shows a host: an IPv4 address as IPv4 (`10.0.0.1`), an IPv6 one in its shortest form
//! (`2001:db8::1`, `::ffff:1.2.3.4`).
use super::ColumnType;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, FixedSizeBinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

pub const INET: ColumnType = ColumnType { name: "inet", extension: "pondra.inet", storage, refusal: None, parse: Some(parse), show: Some(show) };

/// The bytes a value is held in: its family, then its address's 16.
pub const WIDTH: usize = 17;

/// The family byte of an IPv4 address and of an IPv6 one, in that order.
const V4: u8 = 4;
const V6: u8 = 6;

fn storage(_: &[u64]) -> DataType { DataType::FixedSizeBinary(WIDTH as i32) }

/// An address's bytes (`WIDTH`), from its text, refused as Postgres refuses it (22P02); a network, a mask shorter than a
/// host's, is refused as one INET doesn't hold yet (0A000).
pub fn bytes(text: &str) -> anyhow::Result<[u8; WIDTH]> {
    let refused = || crate::codes::coded("22P02", format!("invalid input syntax for type inet: \"{text}\""));
    let (address, mask) = match text.split_once('/') {
        Some((a, m)) => (a, Some(m)),
        None => (text, None),
    };
    let ip: IpAddr = address.parse().map_err(|_| refused())?;
    let host = if ip.is_ipv4() { 32 } else { 128 };
    if let Some(m) = mask {
        match m.parse::<u32>() {
            Ok(m) if m == host => {}
            Ok(m) if m < host => return Err(crate::codes::coded("0A000", format!("{text}: an INET holds one address, a host (/{host}): a network comes with CIDR, later"))),
            _ => return Err(refused()),
        }
    }
    let (family, octets) = match ip {
        IpAddr::V4(v4) => (V4, v4.to_ipv6_mapped().octets()),
        IpAddr::V6(v6) => (V6, v6.octets()),
    };
    let mut out = [family; WIDTH];
    out[1..].copy_from_slice(&octets);
    Ok(out)
}

/// The address its bytes hold (`bytes`).
pub fn address(bytes: &[u8]) -> IpAddr {
    let v6 = Ipv6Addr::from(<[u8; 16]>::try_from(bytes.get(1..).unwrap_or_default()).unwrap_or_default());
    match (bytes.first(), v6.to_ipv4_mapped()) {
        (Some(&V4), Some(v4)) => IpAddr::V4(v4),
        (Some(&V4), None) => IpAddr::V4(Ipv4Addr::UNSPECIFIED), // (never written: `bytes` maps every IPv4 address)
        _ => IpAddr::V6(v6),
    }
}

/// An address's text, from its bytes.
pub fn text(bytes: &[u8]) -> String { address(bytes).to_string() }

/// Each text (Utf8) as an address, or the first that isn't one refused.
fn parse(c: &ArrayRef) -> anyhow::Result<ArrayRef> {
    let read = c.as_string::<i32>().iter().map(|v| v.map(bytes).transpose()).collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(read.into_iter(), WIDTH as i32)?))
}

/// Each address as its text.
fn show(c: &ArrayRef) -> anyhow::Result<ArrayRef> {
    let Some(a) = c.as_fixed_size_binary_opt() else { anyhow::bail!("an INET's values are {WIDTH} bytes, not {}", c.data_type()) };
    Ok(Arc::new(a.iter().map(|v| v.map(text)).collect::<StringArray>()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Text read and shown back as Postgres shows a host; what isn't one is refused in its words.
    #[test]
    fn an_address_is_read_into_16_bytes_and_shown_as_postgres_shows_a_host() {
        for (given, shown) in [("10.0.0.1", "10.0.0.1"), ("10.0.0.1/32", "10.0.0.1"), ("::1", "::1"), ("2001:DB8:0:0:0:0:0:1", "2001:db8::1"), ("::ffff:1.2.3.4", "::ffff:1.2.3.4"), ("fe80::1/128", "fe80::1")] {
            let b = bytes(given).unwrap_or_else(|e| panic!("{given}: {e}"));
            assert_eq!(text(&b), shown, "{given}");
        }
        assert_eq!(bytes("1.2.3.4").unwrap()[..13], [4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff], "IPv4 is its family, then mapped");
        assert!(bytes("10.0.0.1").unwrap() < bytes("10.0.0.2").unwrap() && bytes("9.255.255.255").unwrap() < bytes("10.0.0.0").unwrap(), "ordered by address");
        // As Postgres and DuckDB have it: every IPv4 address before every IPv6 one, and a mapped address apart from its IPv4
        assert!(bytes("255.255.255.255").unwrap() < bytes("::").unwrap() && bytes("10.0.0.1").unwrap() < bytes("::1").unwrap(), "IPv4 first");
        assert_ne!(bytes("::ffff:1.2.3.4").unwrap(), bytes("1.2.3.4").unwrap());
        for bad in ["10.0.0.256", "x", "", "10.0.0.1/33", "10.0.0.1/x", " 10.0.0.1", "1.2.3"] {
            let e = bytes(bad).unwrap_err();
            assert_eq!((crate::codes::of(&e), e.to_string()), ("22P02", format!("invalid input syntax for type inet: \"{bad}\"")), "{bad}");
        }
        let e = bytes("10.0.0.0/8").unwrap_err();
        assert_eq!(crate::codes::of(&e), "0A000");
        assert!(e.to_string().contains("a network comes with CIDR"), "{e}");
    }

    #[test]
    fn a_batch_is_read_and_shown_with_its_nulls() {
        let c: ArrayRef = Arc::new(StringArray::from(vec![Some("10.0.0.1"), None, Some("::1")]));
        let read = parse(&c).unwrap();
        assert_eq!((read.data_type(), read.null_count()), (&DataType::FixedSizeBinary(WIDTH as i32), 1));
        let back = show(&read).unwrap();
        assert_eq!(back.as_string::<i32>().iter().collect::<Vec<_>>(), vec![Some("10.0.0.1"), None, Some("::1")]);
    }
}
