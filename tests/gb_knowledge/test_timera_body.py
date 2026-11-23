"""Timera article body extraction (no network).

Regression: on the current Timera layout `div.entry-content` is the "Related
client case studies" widget; the article lives in <article> as `div.text-content`
blocks.  3 of 9 stored docs were case-study blurbs (552-868 chars), which the
digest then (correctly) found nothing durable in.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from bs4 import BeautifulSoup

from services.gb_knowledge.timera import TimeraConnector

P1 = "TTF prices have retreated since their year-to-date high on 30 October, despite several signals that Europe's winter gas balance remains fragile."
P2 = "European gas prices have corrected from their late-October peak, which is consistent with a repricing of supply risk rather than a change in fundamentals."
P3 = "Physical conditions offer a contrasting signal: storage withdrawals are deepening while LNG loadings only partially offset the shortfall this winter."

NEW_LAYOUT = f"""
<html><body><main class="site-main">
<section class="hero-single-blog"><h1>TTF retreats</h1><p>Short hero lead that is not the body of the piece itself.</p></section>
<article class="col-lg-7">
  <div class="text-content"><p>{P1}</p><p>{P2}</p></div>
  <div class="text-content"><p>Note: chart units $/MMBtu.</p><p>{P3}</p></div>
</article>
<div class="entry-content"><h3>Related client case studies</h3>
  <p>Equity acquisition: market analysis supporting a bid for a large European gas storage portfolio.</p></div>
</main></body></html>
"""

OLD_LAYOUT = f"""
<html><body><main><div class="entry-content"><p>{P1}</p><p>{P2}</p><p>{P3}</p></div></main></body></html>
"""


def _body(html):
    return TimeraConnector()._extract_body(BeautifulSoup(html, "html.parser"))


def test_new_layout_returns_article_blocks_not_case_studies():
    body = _body(NEW_LAYOUT)
    for para in (P1, P2, P3):
        assert para in body
    assert "case studies" not in body.lower()
    assert "Equity acquisition" not in body


def test_old_layout_entry_content_still_works():
    body = _body(OLD_LAYOUT)
    assert P1 in body and P3 in body
