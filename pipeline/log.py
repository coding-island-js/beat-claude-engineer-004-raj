"""Durable, partitioned, append-only event log.

Stands in for Kinesis Data Streams in the AWS design. Same contract:
- A record is acknowledged only after it is on disk (fsync), never before.
- Records in one partition keep their order. Partition key = tenant + visitor,
  so one visitor's events are processed in the order they arrived.
- Every record gets a sequence number. Consumers commit the last sequence
  they finished, so a crash replays from there (at-least-once delivery).

Line format:  <seq>\t<crc32 of json>\t<json>\n
A crash can leave a half-written last line. On open, any line whose CRC does
not match is a torn write: it was never acknowledged, so it is cut off.
"""
import json
import os
import threading
import zlib
import hashlib

QUARANTINE = "q"  # partition for bytes that could not be parsed at all


def partition_for(tenant_id, anonymous_id, n_partitions):
    key = f"{tenant_id}|{anonymous_id}".encode()
    return int(hashlib.md5(key).hexdigest(), 16) % n_partitions


class DurableLog:
    def __init__(self, directory, n_partitions=4):
        self.dir = directory
        self.n = n_partitions
        os.makedirs(directory, exist_ok=True)
        self.names = [str(i) for i in range(n_partitions)] + [QUARANTINE]
        self.next_seq = {}
        self.files = {}
        self.lock = threading.Lock()
        for name in self.names:
            path = self.path(name)
            self.next_seq[name] = self._recover(path)
            self.files[name] = open(path, "ab")

    def path(self, name):
        return os.path.join(self.dir, f"part-{name}.log")

    @staticmethod
    def _recover(path):
        """Return next sequence number; truncate a torn last record."""
        if not os.path.exists(path):
            return 1
        good_bytes, last_seq = 0, 0
        with open(path, "rb") as f:
            for line in f:
                parsed = _parse_line(line)
                if parsed is None:
                    break
                last_seq = parsed[0]
                good_bytes += len(line)
        if good_bytes != os.path.getsize(path):
            with open(path, "r+b") as f:
                f.truncate(good_bytes)
        return last_seq + 1

    def append_many(self, items):
        """items: list of (partition_name, record_dict).
        Writes all, fsyncs each touched partition once (group commit),
        returns list of (partition_name, seq) in the same order."""
        out, touched = [], set()
        with self.lock:
            for name, rec in items:
                seq = self.next_seq[name]
                self.next_seq[name] = seq + 1
                body = json.dumps(rec, separators=(",", ":"), sort_keys=True)
                crc = zlib.crc32(body.encode())
                self.files[name].write(f"{seq}\t{crc}\t{body}\n".encode())
                touched.add(name)
                out.append((name, seq))
            for name in touched:
                f = self.files[name]
                f.flush()
                os.fsync(f.fileno())
        return out

    def head(self):
        return {name: self.next_seq[name] - 1 for name in self.names}

    def close(self):
        for f in self.files.values():
            f.close()


def _parse_line(line):
    try:
        text = line.decode()
        if not text.endswith("\n"):
            return None
        seq_s, crc_s, body = text[:-1].split("\t", 2)
        if zlib.crc32(body.encode()) != int(crc_s):
            return None
        return int(seq_s), json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return None


def read_partition(directory, name, after_seq=0, limit=None):
    """Yield (seq, record) with seq > after_seq. Reads only CRC-valid lines."""
    path = os.path.join(directory, f"part-{name}.log")
    if not os.path.exists(path):
        return
    count = 0
    with open(path, "rb") as f:
        for line in f:
            parsed = _parse_line(line)
            if parsed is None:
                return
            seq, rec = parsed
            if seq <= after_seq:
                continue
            yield seq, rec
            count += 1
            if limit and count >= limit:
                return


class Tailer:
    """Follows one partition file from a byte position, so a long-running
    consumer does not rescan the file on every poll. A line still being
    written (no newline yet) is left for the next poll."""

    def __init__(self, directory, name, after_seq=0):
        self.path = os.path.join(directory, f"part-{name}.log")
        self.pos = 0
        self.last_seq = after_seq
        # skip to the committed offset once, at startup
        for _ in self.poll(after_seq_skip=after_seq):
            pass

    def poll(self, limit=None, after_seq_skip=None):
        if not os.path.exists(self.path):
            return
        count = 0
        with open(self.path, "rb") as f:
            f.seek(self.pos)
            while True:
                line = f.readline()
                if not line or not line.endswith(b"\n"):
                    return
                parsed = _parse_line(line)
                if parsed is None:
                    return
                self.pos += len(line)
                seq, rec = parsed
                if after_seq_skip is not None:
                    if seq > after_seq_skip:
                        # first record past the offset: rewind and stop
                        self.pos -= len(line)
                        return
                    continue
                if seq <= self.last_seq:
                    continue
                self.last_seq = seq
                yield seq, rec
                count += 1
                if limit and count >= limit:
                    return


def head_from_disk(directory, names):
    """Last valid seq per partition, read from disk (used by other processes)."""
    heads = {}
    for name in names:
        last = 0
        for seq, _ in read_partition(directory, name):
            last = seq
        heads[name] = last
    return heads


def rewrite_partition(directory, name, keep):
    """Compaction for GDPR deletion: rewrite a partition keeping only records
    where keep(record) is True. Sequence numbers are preserved, so consumer
    offsets stay valid. Writes a temp file then atomically replaces."""
    path = os.path.join(directory, f"part-{name}.log")
    if not os.path.exists(path):
        return []
    removed = []
    tmp = path + ".compact"
    with open(path, "rb") as src, open(tmp, "wb") as dst:
        for line in src:
            parsed = _parse_line(line)
            if parsed is None:
                break
            if keep(parsed[1]):
                dst.write(line)
            else:
                removed.append(parsed[0])
        dst.flush()
        os.fsync(dst.fileno())
    os.replace(tmp, path)
    return removed
