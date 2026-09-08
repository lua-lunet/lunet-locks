//! The LAL Peer Protocol framing (`src/transport.tl`, mirrored byte for
//! byte) plus the membership/v3 genesis fingerprint (`src/config.tl`,
//! mirrored byte for byte). The envelope is
//! `\0LUNET_ADVISORY_LOCK_PEER\0 | kind | fingerprint(16 hex) | payload`.

pub const PEER_MAGIC: &[u8] = b"\x00LUNET_ADVISORY_LOCK_PEER\x00";
pub const PEER_VRR: u8 = 1;
pub const PEER_APPLICATION: u8 = 2;
pub const FORWARD_REQUEST: u8 = 1;
pub const FORWARD_RESPONSE: u8 = 2;
pub const FORWARD_NOT_LEADER: u8 = 3;
pub const PEER_HEADER_BYTES: usize = PEER_MAGIC.len() + 1 + 16;

const MEMBERSHIP_DOMAIN: &[u8] = b"lunet-advisory-lock/membership/v3\x00";
/// Upstream `src/wire.rs` Tag::Reincarnation; the wire body is the
/// restarted replica's addressing notice.
const REINCARNATION_TAG: u32 = 13;
const REINCARNATION_BYTES: usize = 20 + 1 + 4 + 4;

pub fn encode_peer(kind: u8, fingerprint: &str, payload: &[u8]) -> Vec<u8> {
    let mut packet = Vec::with_capacity(PEER_HEADER_BYTES + payload.len());
    packet.extend_from_slice(PEER_MAGIC);
    packet.push(kind);
    packet.extend_from_slice(fingerprint.as_bytes());
    packet.extend_from_slice(payload);
    packet
}

pub fn decode_peer(packet: &[u8]) -> Option<(u8, &str, &[u8])> {
    if packet.len() < PEER_HEADER_BYTES
        || &packet[..PEER_MAGIC.len()] != PEER_MAGIC
    {
        return None;
    }
    let kind = packet[PEER_MAGIC.len()];
    if kind != PEER_VRR && kind != PEER_APPLICATION {
        return None;
    }
    let fingerprint_start = PEER_MAGIC.len() + 1;
    let fingerprint = std::str::from_utf8(
        &packet[fingerprint_start..fingerprint_start + 16],
    )
    .ok()?;
    if fingerprint.len() != 16 || !fingerprint.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some((kind, fingerprint, &packet[PEER_HEADER_BYTES..]))
}

/// The `Reincarnation(old, new)` pair inside a VRR payload: 20-byte
/// big-endian header (tag, era, view, slot), one-byte body discriminant,
/// two big-endian u32 member ids — 29 bytes total.
pub fn reincarnation_pair(payload: &[u8]) -> Option<(u32, u32)> {
    if payload.len() != REINCARNATION_BYTES {
        return None;
    }
    if u32::from_be_bytes(payload[0..4].try_into().ok()?) != REINCARNATION_TAG {
        return None;
    }
    if payload[20] as u32 != REINCARNATION_TAG {
        return None;
    }
    let old = u32::from_be_bytes(payload[21..25].try_into().ok()?);
    let new = u32::from_be_bytes(payload[25..29].try_into().ok()?);
    Some((old, new))
}

pub fn uuid_bytes(text: &str) -> Option<[u8; 16]> {
    let hex: String = text.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

/// One genesis member's fingerprint fields (the descriptor line's
/// `id`, `name`, `host`, `port`).
pub struct GenesisMember<'a> {
    pub id: u32,
    pub name: &'a str,
    pub host: &'a str,
    pub port: u16,
}

/// The membership/v3 fingerprint: SHA-256 over the domain-separated,
/// length-delimited genesis membership in descriptor line order, first 16
/// lowercase hex chars (`config.tl`'s `genesis_fingerprint`).
pub fn genesis_fingerprint(genesis: &[GenesisMember<'_>]) -> String {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(MEMBERSHIP_DOMAIN);
    encoded.extend_from_slice(genesis.len().to_string().as_bytes());
    encoded.push(b':');
    for node in genesis {
        for field in [
            node.id.to_string(),
            node.name.to_string(),
            node.host.to_string(),
            node.port.to_string(),
        ] {
            encoded.extend_from_slice(field.len().to_string().as_bytes());
            encoded.push(b':');
            encoded.extend_from_slice(field.as_bytes());
        }
    }
    sha256_hex(&encoded)[..16].to_string()
}

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut message = data.to_vec();
    let bit_len = (data.len() as u64) * 8;
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());
    for block in message.chunks(64) {
        let mut w = [0u32; 64];
        for (index, word) in block.chunks(4).enumerate() {
            w[index] = u32::from_be_bytes(word.try_into().expect("4 bytes"));
        }
        for index in 16..64 {
            let s0 = w[index - 15].rotate_right(7)
                ^ w[index - 15].rotate_right(18)
                ^ (w[index - 15] >> 3);
            let s1 = w[index - 2].rotate_right(17)
                ^ w[index - 2].rotate_right(19)
                ^ (w[index - 2] >> 10);
            w[index] = w[index - 16]
                .wrapping_add(s0)
                .wrapping_add(w[index - 7])
                .wrapping_add(s1);
        }
        let mut v = h;
        for index in 0..64 {
            let s1 = v[4].rotate_right(6) ^ v[4].rotate_right(11) ^ v[4].rotate_right(25);
            let choose = (v[4] & v[5]) ^ ((!v[4]) & v[6]);
            let temp1 = v[7]
                .wrapping_add(s1)
                .wrapping_add(choose)
                .wrapping_add(SHA256_K[index])
                .wrapping_add(w[index]);
            let s0 = v[0].rotate_right(2) ^ v[0].rotate_right(13) ^ v[0].rotate_right(22);
            let majority = (v[0] & v[1]) ^ (v[0] & v[2]) ^ (v[1] & v[2]);
            let temp2 = s0.wrapping_add(majority);
            v[7] = v[6];
            v[6] = v[5];
            v[5] = v[4];
            v[4] = v[3].wrapping_add(temp1);
            v[3] = v[2];
            v[2] = v[1];
            v[1] = v[0];
            v[0] = temp1.wrapping_add(temp2);
        }
        for index in 0..8 {
            h[index] = h[index].wrapping_add(v[index]);
        }
    }
    let mut digest = [0u8; 32];
    for (index, word) in h.iter().enumerate() {
        digest[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    digest
}

fn sha256_hex(data: &[u8]) -> String {
    sha256(data).iter().map(|b| format!("{b:02x}")).collect()
}
