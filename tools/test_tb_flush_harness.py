# The TigerBeetle-pattern flush harness tests: the geometry, the ordering,
# the quorum rule, and the percentile math are the four contracts.
import unittest

from tb_flush_harness import (
    MockBackend,
    slot_offset,
    nearest_rank,
    wal_cycle,
    checkpoint_cycle,
    grid_cycle,
    verify_superblock_quorum,
)


class SlotOffsets(unittest.TestCase):
    def test_slot_wraps_at_the_ring_size(self):
        # TB: slot = op % 1024 (journal ring, config.zig:158).
        self.assertEqual(slot_offset(0, 1024, 1_048_576), 0)
        self.assertEqual(slot_offset(1, 1024, 1_048_576), 1_048_576)
        self.assertEqual(slot_offset(1023, 1024, 1_048_576), 1023 * 1_048_576)
        self.assertEqual(slot_offset(1024, 1024, 1_048_576), 0, "the ring wraps in place")

    def test_headers_ring_uses_the_same_slot_number(self):
        # The redundant header lands in the DIFFERENT wal_headers area at
        # the same slot number (256 B slots).
        self.assertEqual(slot_offset(5, 1024, 256), 5 * 256)


class WalCycle(unittest.TestCase):
    def test_body_completes_before_the_header_is_written(self):
        # TB: the header is written from the body's completion callback
        # (journal.zig:1908) — the order on the backend must prove it.
        backend = MockBackend()
        wal_cycle(backend, op=7, body_size=16, header_size=8, ring_slots=4, ring_slot_size=32)
        kinds = [entry[0] for entry in backend.log]
        self.assertEqual(kinds, ["body", "header"])
        # Body: slot 7 % 4 = 3. Header: same slot number, 256-B-class area.
        self.assertEqual(backend.log[0][1], 3 * 32)
        self.assertEqual(backend.log[1][1], 3 * 8)

    def test_cycle_returns_both_latencies(self):
        backend = MockBackend(delay_us=(50, 30))
        result = wal_cycle(backend, op=1, body_size=16, header_size=8, ring_slots=4, ring_slot_size=32)
        self.assertGreaterEqual(result["body_us"], 50)
        self.assertGreaterEqual(result["header_us"], 30)


class SuperblockQuorum(unittest.TestCase):
    def test_three_of_four_copies_verify(self):
        payload = b"x" * 64
        copies = [bytearray(payload) for _ in range(4)]
        self.assertTrue(verify_superblock_quorum(copies, payload_checksum=None))

    def test_two_corrupt_copies_fail_the_3_of_4_rule(self):
        payload = b"x" * 64
        copies = [bytearray(payload) for _ in range(4)]
        copies[0][0:8] = b"corrupt!"
        copies[2][0:8] = b"corrupt!"
        self.assertFalse(verify_superblock_quorum(copies, payload_checksum=None))

    def test_one_corrupt_copy_still_verifies(self):
        payload = b"x" * 64
        copies = [bytearray(payload) for _ in range(4)]
        copies[1][0:8] = b"corrupt!"
        self.assertTrue(verify_superblock_quorum(copies, payload_checksum=None))


class CheckpointCycle(unittest.TestCase):
    def test_superblock_copies_written_last_after_grid(self):
        # TB: grid/replies zones first, the superblock seal LAST.
        backend = MockBackend()
        checkpoint_cycle(backend, grid_blocks=2, grid_block_size=128, superblock_copies=4, superblock_copy_size=64)
        kinds = [entry[0] for entry in backend.log]
        self.assertEqual(kinds[:2], ["grid", "grid"])
        self.assertEqual(kinds[-4:], ["superblock"] * 4)
        offsets = [entry[1] for entry in backend.log[-4:]]
        self.assertEqual(offsets, [0, 64, 128, 192], "copies at copy * superblock_copy_size")


class GridCycle(unittest.TestCase):
    def test_grid_blocks_written_at_stride(self):
        backend = MockBackend()
        grid_cycle(backend, blocks=3, block_size=128, stride=256)
        self.assertEqual([entry[1] for entry in backend.log], [0, 256, 512])


class Percentiles(unittest.TestCase):
    def test_nearest_rank_on_exact_thousand(self):
        samples = list(range(1, 1001))  # 1..1000 us
        self.assertEqual(nearest_rank(samples, 50), 500)
        self.assertEqual(nearest_rank(samples, 99), 990)
        self.assertEqual(nearest_rank(samples, 99.9), 999)
        self.assertEqual(nearest_rank(samples, 99.99), 1000)

    def test_nearest_rank_small_n(self):
        samples = [10, 20, 30, 40]
        self.assertEqual(nearest_rank(samples, 50), 20)
        self.assertEqual(nearest_rank(samples, 90), 40)


if __name__ == "__main__":
    unittest.main()
