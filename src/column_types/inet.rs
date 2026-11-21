//! INET (the internal name IPADDRESS; L6's words, 11-14): one IPv4 or IPv6 address, as Postgres's `inet` and DuckDB's
//! `INET` name it. It is held in 16 bytes (FixedSizeBinary(16)), an IPv4 address as its IPv4-mapped IPv6 one
//! (`::ffff:a.b.c.d`), so values compare, sort, group and join by their bytes. No netmask is kept (CIDR comes later): a host's
//! full mask is taken (`10.0.0.1/32`, `::1/128`) and a network's refused. A value is shown as Postgres shows a host: an
//! IPv4-mapped address as IPv4 (`10.0.0.1`), any other in IPv6's shortest form (`2001:db8::1`).
use super::ColumnType;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, FixedSizeBinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;

pub const INET: ColumnType = ColumnType { name: "inet", extension: "pondra.inet", storage, refusal: None, parse: Some(parse), show: Some(show) };

fn storage(_: &[u64]) -> DataType { DataType::FixedSizeBinary(16) }

/// An address's 16 bytes, from its text, refused as Postgres refuses it (22P02); a network, a mask shorter than a host's, is
/// refused as one INET doesn't hold yet (0A000).
pub fn bytes(text: &str) -> anyhow::Result<[u8; 16]> {
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
    Ok(match ip {
        IpAddr::V4(v4) => v4.to_ipv6_mapped().octets(),
        IpAddr::V6(v6) => v6.octets(),
    })
}

/// An address's text, from its 16 bytes.
pub fn text(bytes: &[u8]) -> String {
    let v6 = Ipv6Addr::from(<[u8; 16]>::try_from(bytes).unwrap_or_default());
    match v6.to_ipv4_mapped() {
        Some(v4) => v4.to_string(),
        None => v6.to_string(),
    }
}

/// Each text (Utf8) as an address, or the first that isn't one refused.
fn parse(c: &ArrayRef) -> anyhow::Result<ArrayRef> {
    let read = c.as_string::<i32>().iter().map(|v| v.map(bytes).transpose()).collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(read.into_iter(), 16)?))
}

/// Each address as its text.
fn show(c: &ArrayRef) -> anyhow::Result<ArrayRef> {
    let Some(a) = c.as_fixed_size_binary_opt() else { anyhow::bail!("an INET's values are 16 bytes, not {}", c.data_type()) };
    Ok(Arc::new(a.iter().map(|v| v.map(text)).collect::<StringArray>()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Text read and shown back as Postgres shows a host; what isn't one is refused in its words.
    #[test]
    fn an_address_is_read_into_16_bytes_and_shown_as_postgres_shows_a_host() {
        for (given, shown) in [("10.0.0.1", "10.0.0.1"), ("10.0.0.1/32", "10.0.0.1"), ("::1", "::1"), ("2001:DB8:0:0:0:0:0:1", "2001:db8::1"), ("::ffff:1.2.3.4", "1.2.3.4"), ("fe80::1/128", "fe80::1")] {
            let b = bytes(given).unwrap_or_else(|e| panic!("{given}: {e}"));
            assert_eq!(text(&b), shown, "{given}");
        }
        assert_eq!(bytes("1.2.3.4").unwrap()[..12], [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff], "IPv4 is mapped");
        assert!(bytes("10.0.0.1").unwrap() < bytes("10.0.0.2").unwrap() && bytes("9.255.255.255").unwrap() < bytes("10.0.0.0").unwrap(), "ordered by address");
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
        assert_eq!((read.data_type(), read.null_count()), (&DataType::FixedSizeBinary(16), 1));
        let back = show(&read).unwrap();
        assert_eq!(back.as_string::<i32>().iter().collect::<Vec<_>>(), vec![Some("10.0.0.1"), None, Some("::1")]);
    }
}
