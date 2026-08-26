"""Reproduces the standard-Bloom-filter deletion trap (naive bit-clearing
breaks a colliding symbol) and confirms CountingBloomFilter survives the
same collision via decrement instead of clear."""
import uuid

import pytest
import redis.asyncio as redis

from pnl.cache.bloom import BloomFilter, CountingBloomFilter


def _find_colliding_pair(bloom) -> tuple[str, str]:
    """With size_bits=4 and num_hashes=1, a handful of candidates are
    guaranteed (pigeonhole) to collide on the same single bit/slot."""
    seen: dict[int, str] = {}
    for i in range(50):
        candidate = f"SYM{i}"
        pos = bloom._bit_positions(candidate)[0]
        if pos in seen:
            return seen[pos], candidate
        seen[pos] = candidate
    raise AssertionError("could not find a colliding pair — unexpected with size_bits=4")


@pytest.mark.asyncio
async def test_standard_bloom_filter_breaks_on_naive_deletion():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    key = f"bloom:collision-test:{uuid.uuid4().hex[:8]}"
    await r.delete(key)
    bloom = BloomFilter(r, key, size_bits=4, num_hashes=1)

    symbol_a, symbol_b = _find_colliding_pair(bloom)
    await bloom.add(symbol_a)
    await bloom.add(symbol_b)
    assert await bloom.might_contain(symbol_a)
    assert await bloom.might_contain(symbol_b)

    # Naive "deletion" of symbol_a: clear its bit(s) directly — the only
    # option a standard Bloom filter offers, and exactly the trap this
    # stage exists to fix.
    for pos in bloom._bit_positions(symbol_a):
        await r.setbit(key, pos, 0)

    assert not await bloom.might_contain(symbol_a)
    assert not await bloom.might_contain(symbol_b), (
        "test setup issue: expected the collision to also (incorrectly) "
        "clear symbol_b — if it didn't, this test isn't exercising the trap"
    )

    await r.delete(key)
    await r.aclose()


@pytest.mark.asyncio
async def test_counting_bloom_filter_survives_the_same_collision():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    key = f"cbloom:collision-test:{uuid.uuid4().hex[:8]}"
    await r.delete(key)
    bloom = CountingBloomFilter(r, key, size_bits=4, num_hashes=1)

    symbol_a, symbol_b = _find_colliding_pair(bloom)
    await bloom.add(symbol_a)
    await bloom.add(symbol_b)
    assert await bloom.might_contain(symbol_a)
    assert await bloom.might_contain(symbol_b)

    await bloom.remove(symbol_a)

    assert not await bloom.might_contain(symbol_a)
    assert await bloom.might_contain(symbol_b), (
        "removing symbol_a incorrectly broke symbol_b even though their "
        "shared counter should only decrement (2 -> 1), not clear to 0"
    )

    await r.delete(key)
    await r.aclose()
