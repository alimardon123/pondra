//! The body of a request that carries tables of its own (`application/vnd.pondra.request`, invariant 87): the JSON
//! (`{"sql", "tables"}`, the names of the tables), then each table as its length (8 bytes, little-endian) and an Arrow IPC
//! stream, in the JSON's order. The JSON's own length comes first (4 bytes, little-endian). A node's `/sql` reads it
//! (`routines::Request::read`); a page sends it to its node with the rows of its own tables the query names
//! (`page::Page`); the Python client writes the same bytes. A request with no tables of its own is plain SQL or JSON.

/// The content type of a body with tables of its own (`write`, `read`).
pub const CONTENT_TYPE: &str = "application/vnd.pondra.request";

/// A request body: the JSON `head`, then each of `tables` (an Arrow IPC stream) after it, each with its length.
pub fn write(head: &[u8], tables: &[Vec<u8>]) -> bytes::Bytes {
    let mut body = Vec::with_capacity(4 + head.len() + tables.iter().map(|t| 8 + t.len()).sum::<usize>());
    body.extend_from_slice(&(head.len() as u32).to_le_bytes());
    body.extend_from_slice(head);
    for table in tables {
        body.extend_from_slice(&(table.len() as u64).to_le_bytes());
        body.extend_from_slice(table);
    }
    body.into()
}

/// The JSON, and every stream after it in order, each a slice of `body`. A length that runs past the body's end means the
/// body was cut short ("the request ends too soon"): none of its tables is read from the bytes that are there.
pub fn read(body: &[u8]) -> anyhow::Result<(&[u8], Vec<&[u8]>)> {
    let short = || anyhow::anyhow!("the request ends too soon");
    let n = u32::from_le_bytes(body.get(..4).ok_or_else(short)?.try_into()?) as usize;
    // (`get` on each part, not `4 + n`: a length too big for the body is short, and cannot overflow a 32-bit address space)
    let after_length = body.get(4..).ok_or_else(short)?;
    let head = after_length.get(..n).ok_or_else(short)?;
    let mut rest = &after_length[n..];
    let mut streams = vec![];
    while !rest.is_empty() {
        let len = usize::try_from(u64::from_le_bytes(rest.get(..8).ok_or_else(short)?.try_into()?)).map_err(|_| short())?;
        streams.push(rest.get(8..).and_then(|r| r.get(..len)).ok_or_else(short)?);
        rest = &rest[8 + len..];
    }
    Ok((head, streams))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_is_written_is_read_back() {
        let body = write(b"{\"sql\":\"x\"}", &[vec![1, 2, 3], vec![]]);
        let (head, streams) = read(&body).unwrap();
        assert_eq!(head, &b"{\"sql\":\"x\"}"[..]);
        assert_eq!(streams, vec![&[1u8, 2, 3][..], &[0u8; 0][..]]);
    }

    #[test]
    fn a_length_past_the_end_is_the_request_ending_too_soon() {
        let body = write(b"{}", &[vec![1, 2, 3]]);
        for cut in [2, 5, 10, 16] {
            let err = read(&body[..cut]).unwrap_err();
            assert_eq!(err.to_string(), "the request ends too soon", "cut at {cut}");
        }
    }
}
