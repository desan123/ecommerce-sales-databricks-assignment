# COMMAND ----------

# MAGIC %run /notebooks/util_lib/utils

# COMMAND ----------

class TestNormalizeUnicode:
    def test_none_returns_none(self):
        try:
            assert normalize_unicode(None) is None
        except AssertionError as e:
            print("Test Failed")
        else:
            print("Test Passed")

    def test_plain_ascii_unchanged(self):
        try:
            assert normalize_unicode("hello") == "hello"
        except AssertionError as e:
            print("Test Failed")
        else:
            print("Test Passed")

    def test_nfc_normalisation(self):
        decomposed = "Jose\u0301"    # José (NFD)
        normalised  = normalize_unicode(decomposed)
        try:
            assert normalised == "José"
            assert len(normalised) == 4   # NFC: 4 chars
        except AssertionError as e:
            print("Test Failed")
        else:
            print("Test Passed")

    def test_chinese_characters_preserved(self):
        text = "数据工程"
        try:
            assert normalize_unicode(text) == text
        except AssertionError as e:
            print("Test Failed")
        else:
            print("Test Passed")

    def test_empty_string(self):
        try:
            assert normalize_unicode("") == ""
        except AssertionError as e:
            print("Test Failed")
        else:
            print("Test Passed")