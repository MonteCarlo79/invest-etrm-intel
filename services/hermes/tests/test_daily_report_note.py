import services.hermes.market_report as mr


def test_report_to_markdown_renders_sections():
    report = {
        "executive_summary": "今日市场震荡。",
        "sections": [
            {
                "title": "山东现货",
                "content": "午后低价频现。",
                "items": [{"title": "光伏出力新高", "content": "14时出力达32GW", "source": "山东省调", "date": "2026-08-05"}],
            },
            {"title": "山西现货", "content": "价格平稳。"},
        ],
    }
    md = mr._report_to_markdown(report, "2026年08月06日")
    assert md.startswith("# 电力市场日报 — 2026年08月06日")
    assert "今日市场震荡。" in md
    assert "## 山东现货" in md
    assert "午后低价频现。" in md
    assert "- **光伏出力新高**（山东省调, 2026-08-05）：14时出力达32GW" in md
    assert "## 山西现货" in md
