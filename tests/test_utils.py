import pytest
import sys
sys.path.insert(0, "../assignment/notebooks/util_lib/")
from utils import normalize_unicode

class TestNormalizeUnicode:

    def test_none_returns_none(self):
        assert normalize_unicode(None) is None

    def test_plain_ascii_unchanged(self):
        assert normalize_unicode("hello") == "hello"

    def test_nfc_normalisation(self):
        decomposed = "Jose\u0301"    # José (NFD)
        normalised  = normalize_unicode(decomposed)
        assert normalised == "José"
        assert len(normalised) == 4   # NFC: 4 chars

    def test_chinese_characters_preserved(self):
        text = "数据工程"
        assert normalize_unicode(text) == text

    def test_empty_string(self):
        assert normalize_unicode("") == ""