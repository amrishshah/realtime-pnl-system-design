"""No false negatives for anything added; false-positive rate stays within
the expected ballpark for the chosen size/k."""
import uuid

import pytest
import redis.asyncio as redis

from pnl.cache.bloom import BloomFilter


@pytest.mark.asyncio
async def test_no_false_negatives():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    key = f"bloom:test:{uuid.uuid4().hex[:8]}"
    await r.delete(key)
    bloom = BloomFilter(r, key, size_bits=100_000, num_hashes=7)

    symbols = [f"SYM{i}" for i in range(500)]
    for s in symbols:
        await bloom.add(s)

    for s in symbols:
        assert await bloom.might_contain(s), f"false negative for {s} — must never happen"

    await r.delete(key)
    await r.aclose()


@pytest.mark.asyncio
async def test_unadded_symbol_usually_absent():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    key = f"bloom:test:{uuid.uuid4().hex[:8]}"
    await r.delete(key)
    bloom = BloomFilter(r, key, size_bits=1_000_000, num_hashes=7)

    for i in range(1000):
        await bloom.add(f"HELD{i}")

    false_positives = 0
    trials = 2000
    for i in range(trials):
        if await bloom.might_contain(f"NOTHELD{i}"):
            false_positives += 1

    # At 1000 items / 1M bits / k=7, expected FP rate is well under 1%.
    # Allow generous headroom to avoid test flakiness.
    assert false_positives / trials < 0.05, (
        f"false-positive rate {false_positives / trials:.2%} is far higher "
        "than expected for this size/k — check your hashing/bit indexing"
    )

    await r.delete(key)
    await r.aclose()
