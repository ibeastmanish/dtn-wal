//! On-disk frame format.
//!
//! Every record in a segment is one frame: a fixed-size 24-byte header followed by the payload.
//!
//! ```text
//! offset  size  field
//!      0     4  magic     b"DTNW"
//!      4     1  version   1
//!      5     1  kind      record type (see `record.rs`)
//!      6     2  reserved  must be zero
//!      8     8  seq       sequence number, +1 for every frame in the log (little-endian)
//!     16     4  len       payload length in bytes (little-endian)
//!     20     4  crc       CRC32C over header bytes 0..20 followed by the payload (little-endian)
//!     24   len  payload
//! ```
//!
//! The CRC covers the header as well as the payload, so a flipped bit in the sequence number
//! or length is caught just like one in the data. CRC32C is computed by the `crc32c` crate,
//! which uses the SSE4.2 `crc32` instruction on x86-64 and the ARMv8 CRC32C instructions on
//! AArch64, with a software fallback elsewhere.

pub(crate) const MAGIC: [u8; 4] = *b"DTNW";
pub(crate) const VERSION: u8 = 1;
pub(crate) const HEADER_LEN: usize = 24;
/// Upper bound on a payload, so a damaged length field can never cause a huge allocation.
pub(crate) const MAX_PAYLOAD: u32 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    pub kind: u8,
    pub seq: u64,
    pub len: u32,
}

/// Result of trying to read one frame at an offset.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Decoded<'a> {
    Frame {
        header: Header,
        payload: &'a [u8],
        /// Offset of the byte after this frame.
        end: usize,
    },
    /// The buffer ends before the frame does (a torn or unfinished write).
    Incomplete,
    /// The bytes at this offset are not a valid frame.
    Invalid(&'static str),
}

/// Appends one encoded frame to `out`.
pub(crate) fn encode(kind: u8, seq: u64, payload: &[u8], out: &mut Vec<u8>) {
    assert!(payload.len() <= MAX_PAYLOAD as usize, "frame payload too large");
    let start = out.len();
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.push(kind);
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    let crc = crc32c::crc32c_append(crc32c::crc32c(&out[start..start + 20]), payload);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
}

/// Reads the frame starting at `offset`.
pub(crate) fn decode(buf: &[u8], offset: usize) -> Decoded<'_> {
    let rest = &buf[offset.min(buf.len())..];
    if rest.len() < HEADER_LEN {
        return Decoded::Incomplete;
    }
    let h = &rest[..HEADER_LEN];
    if h[0..4] != MAGIC {
        return Decoded::Invalid("bad magic");
    }
    if h[4] != VERSION {
        return Decoded::Invalid("unknown frame version");
    }
    if h[6] != 0 || h[7] != 0 {
        return Decoded::Invalid("reserved bytes not zero");
    }
    let seq = u64::from_le_bytes(h[8..16].try_into().unwrap());
    let len = u32::from_le_bytes(h[16..20].try_into().unwrap());
    let stored_crc = u32::from_le_bytes(h[20..24].try_into().unwrap());
    if len > MAX_PAYLOAD {
        return Decoded::Invalid("payload length out of range");
    }
    let end = HEADER_LEN + len as usize;
    if rest.len() < end {
        return Decoded::Incomplete;
    }
    let payload = &rest[HEADER_LEN..end];
    let crc = crc32c::crc32c_append(crc32c::crc32c(&h[..20]), payload);
    if crc != stored_crc {
        return Decoded::Invalid("checksum mismatch");
    }
    Decoded::Frame {
        header: Header {
            kind: h[5],
            seq,
            len,
        },
        payload,
        end: offset + end,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_is_fixed_size() {
        for len in [0usize, 1, 7, 300] {
            let mut out = Vec::new();
            encode(3, 42, &vec![0xAB; len], &mut out);
            assert_eq!(out.len(), HEADER_LEN + len);
        }
    }

    #[test]
    fn round_trip() {
        let mut out = vec![0xEE; 5]; // frames can start at any offset
        encode(1, 7, b"hello", &mut out);
        encode(2, 8, b"", &mut out);
        let Decoded::Frame { header, payload, end } = decode(&out, 5) else { panic!() };
        assert_eq!(header, Header { kind: 1, seq: 7, len: 5 });
        assert_eq!(payload, b"hello");
        let Decoded::Frame { header, payload, end: end2 } = decode(&out, end) else { panic!() };
        assert_eq!(header, Header { kind: 2, seq: 8, len: 0 });
        assert!(payload.is_empty());
        assert_eq!(end2, out.len());
        assert_eq!(decode(&out, end2), Decoded::Incomplete);
    }

    #[test]
    fn every_single_bit_flip_is_detected() {
        let mut frame = Vec::new();
        encode(1, 99, b"bundle payload with some bytes", &mut frame);
        for bit in 0..frame.len() * 8 {
            let mut damaged = frame.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert!(
                !matches!(decode(&damaged, 0), Decoded::Frame { .. }),
                "flip of bit {bit} was not detected"
            );
        }
    }

    #[test]
    fn every_truncation_is_incomplete() {
        let mut frame = Vec::new();
        encode(1, 1, b"0123456789", &mut frame);
        for cut in 0..frame.len() {
            assert_eq!(decode(&frame[..cut], 0), Decoded::Incomplete, "cut at {cut}");
        }
    }
}
