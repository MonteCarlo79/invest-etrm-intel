"""Timera listing-page link extraction — only /blog/<slug>/ permalinks count.

Regression: the blog page dropped its <article> wrappers, so the scraper fell
through to "every same-domain <a>" and ingested nav pages (our-team, careers,
privacy-policy, capabilities/...) into the KB every night.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from bs4 import BeautifulSoup

from services.gb_knowledge.timera import TimeraConnector

BLOG = "https://timera-energy.com/blog/"

LISTING_HTML = """
<html><body>
<nav>
  <a href="https://timera-energy.com">Home</a>
  <a href="https://timera-energy.com/capabilities/">Capabilities</a>
  <a href="https://timera-energy.com/capabilities/power/">Power</a>
  <a href="https://timera-energy.com/our-team/">Team</a>
  <a href="/blog">Blog</a>
  <a href="https://timera-energy.com/publications/">Publications</a>
  <a href="https://timera-energy.com/contact-us/">Contact</a>
</nav>
<div class="post-grid">
  <div class="post-card"><a href="https://timera-energy.com/blog/ttf-retreats-as-winter-gas-risks-build/">A</a></div>
  <div class="post-card"><a href="/blog/ccus-faces-a-tougher-affordability-test/">B</a></div>
  <div class="post-card"><a href="https://timera-energy.com/blog/ttf-retreats-as-winter-gas-risks-build/">A again</a></div>
</div>
<a href="https://timera-energy.com/blog/page/2/">Next</a>
<a href="https://timera-energy.com/blog/category/power/">Power category</a>
<footer>
  <a href="https://timera-energy.com/careers/">Careers</a>
  <a href="https://timera-energy.com/privacy-policy/">Privacy</a>
  <a href="/disclaimer/">Disclaimer</a>
</footer>
</body></html>
"""


def test_only_blog_permalinks_are_extracted():
    soup = BeautifulSoup(LISTING_HTML, "html.parser")
    links = TimeraConnector()._extract_article_links(soup, BLOG)
    assert links == [
        "https://timera-energy.com/blog/ttf-retreats-as-winter-gas-risks-build/",
        "https://timera-energy.com/blog/ccus-faces-a-tougher-affordability-test/",
    ]


def test_nav_and_listing_urls_are_not_articles():
    c = TimeraConnector()
    for href in (
        "https://timera-energy.com/our-team/",
        "https://timera-energy.com/capabilities/power/",
        "https://timera-energy.com/careers/",
        "/disclaimer/",
        "/blog",
        "https://timera-energy.com/blog/",
        "https://timera-energy.com/blog/page/3/",
        "https://timera-energy.com/blog/category/power/",
    ):
        assert not c._is_article_url(href, BLOG), href
