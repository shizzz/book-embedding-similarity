import unittest

from app.parsers.chunk.chunk_strategy import ChunkStrategy
from app.parsers.chunk.description_strategy import DescriptionStrategy
from app.parsers.chunk.passage_strategy import PassageStrategy
from app.parsers.chunk.title_strategy import TitleStrategy


class DummyTokenizer:
    def __init__(self):
        self.calls = []

    def encode(self, text, add_special_tokens=False):
        self.calls.append((text, add_special_tokens))
        # Simple 1:1 char -> int mapping for tests, not important semantically
        return list(range(len(text)))


class TestChunkStrategyBase(unittest.TestCase):
    def test_prepare_returns_prepared_text(self):
        class DummyStrategy(ChunkStrategy):
            def split(self, tokens, max_tokens, min_tokens, overlap, single_chunk_mode):
                return []

        strategy = DummyStrategy()
        prepared = strategy.prepare([1, 2, 3])

        # Lazily import here to avoid circular import issues at module import time
        from app.parsers.chunk.prepared_text import PreparedText

        self.assertIsInstance(prepared, PreparedText)
        self.assertIs(prepared.strategy, strategy)
        self.assertEqual(prepared.tokens, [1, 2, 3])

    def test_prefix_tokens_initialized_when_tokenizer_provided(self):
        tokenizer = DummyTokenizer()

        class PrefixedStrategy(ChunkStrategy):
            prefix = "abc"

            def split(self, tokens, max_tokens, min_tokens, overlap, single_chunk_mode):
                return []

        strategy = PrefixedStrategy(tokenizer=tokenizer)

        self.assertEqual(strategy.prefix_tokens, list(range(len("abc"))))
        self.assertEqual(tokenizer.calls, [("abc", False)])


class TestDescriptionStrategy(unittest.TestCase):
    def setUp(self):
        self.tokenizer = DummyTokenizer()
        self.strategy = DescriptionStrategy(tokenizer=self.tokenizer)

    def test_split_empty_tokens_returns_empty_list(self):
        result = self.strategy.split([], max_tokens=10, min_tokens=1, overlap=0, single_chunk_mode=False)
        self.assertEqual(result, [])

    def test_split_single_chunk_with_prefix_when_short(self):
        tokens = [100, 101, 102]
        prefix_len = len(self.strategy.prefix_tokens)
        max_tokens = prefix_len + len(tokens)

        result = self.strategy.split(tokens, max_tokens=max_tokens, min_tokens=1, overlap=0, single_chunk_mode=False)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][:prefix_len], self.strategy.prefix_tokens)
        self.assertEqual(result[0][prefix_len:], tokens)

    def test_split_truncates_to_max_tokens_minus_prefix(self):
        # make tokens longer than allowed
        tokens = list(range(20))
        prefix_len = len(self.strategy.prefix_tokens)
        max_tokens = prefix_len + 5

        result = self.strategy.split(tokens, max_tokens=max_tokens, min_tokens=1, overlap=0, single_chunk_mode=False)

        self.assertEqual(len(result), 1)
        # total length must not exceed max_tokens
        self.assertLessEqual(len(result[0]), max_tokens)
        # payload part length should be max_tokens - prefix_len
        self.assertEqual(len(result[0]) - prefix_len, max_tokens - prefix_len)


class TestTitleStrategy(unittest.TestCase):
    def setUp(self):
        self.tokenizer = DummyTokenizer()
        self.strategy = TitleStrategy(tokenizer=self.tokenizer)

    def test_always_returns_single_chunk_with_prefix(self):
        tokens = [1, 2, 3, 4]
        prefix_len = len(self.strategy.prefix_tokens)

        result = self.strategy.split(tokens, max_tokens=2, min_tokens=10, overlap=5, single_chunk_mode=False)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][:prefix_len], self.strategy.prefix_tokens)
        self.assertEqual(result[0][prefix_len:], tokens)


class TestPassageStrategy(unittest.TestCase):
    def setUp(self):
        self.tokenizer = DummyTokenizer()
        self.strategy = PassageStrategy(tokenizer=self.tokenizer)

    def _strip_prefix(self, chunk):
        prefix_len = len(self.strategy.prefix_tokens)
        self.assertEqual(chunk[:prefix_len], self.strategy.prefix_tokens)
        return chunk[prefix_len:]

    def test_empty_tokens_returns_empty_list(self):
        result = self.strategy.split([], max_tokens=10, min_tokens=1, overlap=0, single_chunk_mode=False)
        self.assertEqual(result, [])

    def test_single_chunk_when_within_max_and_above_min(self):
        tokens = list(range(8))
        prefix_len = len(self.strategy.prefix_tokens)
        max_tokens = prefix_len + len(tokens)

        result = self.strategy.split(tokens, max_tokens=max_tokens, min_tokens=3, overlap=0, single_chunk_mode=False)

        self.assertEqual(len(result), 1)
        payload = self._strip_prefix(result[0])
        self.assertEqual(payload, tokens)

    def test_too_short_for_single_chunk_returns_empty_when_not_single_chunk_mode(self):
        tokens = [1, 2]
        prefix_len = len(self.strategy.prefix_tokens)
        max_tokens = prefix_len + len(tokens)

        result = self.strategy.split(tokens, max_tokens=max_tokens, min_tokens=5, overlap=0, single_chunk_mode=False)

        self.assertEqual(result, [])

    def test_too_short_but_single_chunk_mode_forces_return(self):
        tokens = [1, 2]
        prefix_len = len(self.strategy.prefix_tokens)
        max_tokens = prefix_len + len(tokens)

        result = self.strategy.split(tokens, max_tokens=max_tokens, min_tokens=5, overlap=0, single_chunk_mode=True)

        self.assertEqual(len(result), 1)
        payload = self._strip_prefix(result[0])
        self.assertEqual(payload, tokens)

    def test_long_text_produces_multiple_chunks_with_overlap_respected(self):
        # create a long sequence of tokens
        tokens = list(range(50))
        prefix_len = len(self.strategy.prefix_tokens)
        max_tokens = prefix_len + 10  # so max_chunk_len = 10
        min_tokens = 5
        overlap = 2

        result = self.strategy.split(
            tokens,
            max_tokens=max_tokens,
            min_tokens=min_tokens,
            overlap=overlap,
            single_chunk_mode=False,
        )

        self.assertGreater(len(result), 1)

        payload_chunks = [self._strip_prefix(c) for c in result]

        # each chunk should respect min and max size
        for chunk in payload_chunks:
            self.assertGreaterEqual(len(chunk), min_tokens)
            self.assertLessEqual(len(chunk), max_tokens - prefix_len)

        # chunks should cover the full range without exceeding bounds
        all_indices = [i for chunk in payload_chunks for i in chunk]
        self.assertGreaterEqual(min(all_indices), 0)
        self.assertLessEqual(max(all_indices), max(tokens))

    def test_chunk_lengths_and_coverage_across_many_variants(self):
        """
        Объединённый тест для множества вариаций вокруг max_tokens=512:
        - длины чанков не должны сильно различаться
        - каждый чанк в пределах [min_tokens, max_chunk_len]
        - покрытие исходных токенов достаточно высокое
        """
        prefix_len = len(self.strategy.prefix_tokens)

        # Базовые сценарии (были отдельными тестами)
        scenarios = [
            dict(n=900, min_tokens=200, overlap=64),   # легко укладываемся в ~2-3 чанка
            dict(n=1600, min_tokens=256, overlap=64),  # близко к лимитам, много чанков
            dict(n=1700, min_tokens=128, overlap=32),  # "обычный" длинный текст
            dict(n=2000, min_tokens=64, overlap=32),   # min маленький, много вариантов шагов
        ]

        # Дополнительные вариации (детерминированно, десятки кейсов)
        for n in range(600, 2601, 100):          # 0 → 2600 с шагом 100
            for min_tokens in range(64, 257, 32): # 64 → 256 с шагом 32
                for overlap in range(16, 65, 16): # 16 → 64 с шагом 16
                    # базовая валидация
                    if overlap >= min_tokens:
                        continue
                    if min_tokens >= n:
                        continue

                    scenarios.append(
                        dict(
                            n=n,
                            min_tokens=min_tokens,
                            overlap=overlap
                        )
                    )

        for sc in scenarios:
            n = sc["n"]
            min_tokens = sc["min_tokens"]
            overlap = sc["overlap"]

            tokens = list(range(n))
            max_tokens = prefix_len + 512
            max_chunk_len = max_tokens - prefix_len

            result = self.strategy.split(
                tokens,
                max_tokens=max_tokens,
                min_tokens=min_tokens,
                overlap=overlap,
                single_chunk_mode=False,
            )

            self.assertGreater(len(result), 1, msg=f"Expected multiple chunks for scenario {sc}")

            payload_chunks = [self._strip_prefix(c) for c in result]

            effective_min = min(min_tokens, max_chunk_len)
            for chunk in payload_chunks:
                self.assertGreaterEqual(len(chunk), effective_min, msg=f"Chunk < min for {sc}")
                self.assertLessEqual(len(chunk), max_chunk_len, msg=f"Chunk > max for {sc}")

            covered_indices = set(i for chunk in payload_chunks for i in chunk)
            self.assertGreater(len(covered_indices), len(tokens) * 0.85, msg=f"Coverage too low for {sc}")

            lengths = [len(c) for c in payload_chunks]
            max_spread = max(64, overlap + 16)
            self.assertLessEqual(
                max(lengths) - min(lengths),
                max_spread,
                msg=f"Lengths vary too much for {sc}: {lengths}",
            )


if __name__ == "__main__":
    unittest.main()

