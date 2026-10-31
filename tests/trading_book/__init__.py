# Package marker: qualifies this dir's modules as trading_book.test_*, so
# test_registry.py / test_waterfall.py no longer collide with the same
# basenames in tests/interconnector/ and tests/asset_risk/ (pytest
# import-file mismatch under a full-suite run).
