// Journal record parser: fixed 61-byte big-endian records with CRC-32 IEEE.
// Mirrors ext/advisory_lock/src/journal.rs parse_file() — stops cleanly at
// the first invalid or short record (corrupt-tail tolerance).

const RECORD_SIZE = 61;
const MAGIC = "LKE1";
const KIND_HOLD = 1;
const KIND_RENEW = 2;
const KIND_RELEASE = 3;

// CRC-32 IEEE table (polynomial 0xEDB88320, reflected).
const CRC32_TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
  let c = i;
  for (let j = 0; j < 8; j++) {
    c = (c & 1) ? ((c >>> 1) ^ 0xEDB88320) : (c >>> 1);
  }
  CRC32_TABLE[i] = c;
}

/** Compute CRC-32 IEEE over a byte range in a Uint8Array. */
export function crc32(data, start = 0, end = data.length) {
  let crc = 0xFFFFFFFF;
  for (let i = start; i < end; i++) {
    crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ data[i]) & 0xFF];
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

function kindName(k) {
  if (k === KIND_HOLD) return "hold";
  if (k === KIND_RENEW) return "renew";
  if (k === KIND_RELEASE) return "release";
  return null;
}

function bytesToHex(u8, offset, len) {
  let s = "";
  for (let i = offset; i < offset + len; i++) {
    s += u8[i].toString(16).padStart(2, "0");
  }
  return s;
}

/**
 * Parse all valid journal records from an ArrayBuffer. Stops at the first
 * invalid magic, bad CRC, unknown kind, or short tail. Returns an array of
 * {kind, ts, lockId, leaseId, holder, expiry}.
 */
export function parseRecords(buffer) {
  const events = [];
  if (!(buffer instanceof ArrayBuffer) || buffer.byteLength < RECORD_SIZE) {
    return events;
  }
  const view = new DataView(buffer);
  const u8 = new Uint8Array(buffer);
  let offset = 0;

  while (offset + RECORD_SIZE <= buffer.byteLength) {
    // Check magic "LKE1".
    if (
      u8[offset] !== 0x4C ||     // L
      u8[offset + 1] !== 0x4B || // K
      u8[offset + 2] !== 0x45 || // E
      u8[offset + 3] !== 0x31    // 1
    ) {
      break;
    }

    // Verify CRC-32 over payload region [offset+8 .. offset+57).
    const storedCrc = view.getUint32(offset + 57, false); // big-endian
    const computedCrc = crc32(u8, offset + 8, offset + 57);
    if (storedCrc !== computedCrc) {
      break;
    }

    const kind = u8[offset + 8];
    const name = kindName(kind);
    if (name === null) {
      break;
    }

    const ts = Number(view.getBigUint64(offset + 9, false));
    const lockId = Number(view.getBigUint64(offset + 17, false));
    const leaseId = Number(view.getBigUint64(offset + 25, false));
    const holder = bytesToHex(u8, offset + 33, 16);
    const expiry = Number(view.getBigUint64(offset + 49, false));

    events.push({ kind: name, ts, lockId, leaseId, holder, expiry });
    offset += RECORD_SIZE;
  }

  return events;
}
