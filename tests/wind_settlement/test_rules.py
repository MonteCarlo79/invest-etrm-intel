# -*- coding: utf-8 -*-
"""Tests for zone-aware CfD settlement (2026 Mengxi rules, 规则体系 第十四条)."""
from __future__ import annotations

import pandas as pd
import pytest


class TestZoneOf:
    def z(self, **kw):
        from services.wind_settlement.rules import zone_of
        return zone_of(**kw)

    def test_east_regions(self):
        for region in ("呼和浩特", "乌兰察布", "锡林郭勒"):
            assert self.z(channel="省内", trade_type="年度挂牌全天直线交易",
                          consumer_unit="翔福新能源", region=region, bureau="") == "east"

    def test_east_bureau_beats_generator_region(self):
        """Jun–Jul files put 鄂尔多斯 (generator region) in 所属地区 for ALL rows —
        bureau must win (乌兰察布电业局 user → east)."""
        assert self.z(channel="省内", trade_type="多年期绿电(非园区)协商交易",
                      consumer_unit="瑞濠科技(乌兰察布电业局)(铁合金)",
                      region="鄂尔多斯", bureau="乌兰察布电业局") == "east"

    def test_west_regions(self):
        for region in ("鄂尔多斯", "包头", "乌海", "巴彦淖尔", "薛家湾", "阿拉善"):
            assert self.z(channel="省内", trade_type="年度挂牌全天直线交易",
                          consumer_unit="某用户", region=region, bureau="") == "west"

    def test_cross_province_is_sending_side_west(self):
        """省间售电 → 送出节点所在区域 (悦盛 = 呼包以西)."""
        assert self.z(channel="跨省", trade_type="2026年1-12月蒙西送北京天津多月省间绿电",
                      consumer_unit="bj-北京聚海讯达", region=None, bureau="") == "west"

    def test_chao_gao_ya_outflow_is_west(self):
        """超高压供电局 = 外送合约 → 送出侧以西."""
        assert self.z(channel="省内", trade_type="年度跨省跨区新能源挂牌",
                      consumer_unit="内蒙古电力（集团）有限责任公司(超高压供",
                      region="", bureau="超高压供电局") == "west"

    def test_grid_proxy_purchase_is_system(self):
        """电网代理工商业 → 全网统一结算点电价."""
        assert self.z(channel="省内", trade_type="月度电网代购（工商业）新能源挂牌交易",
                      consumer_unit="内蒙古电力（集团）", region=None, bureau="内蒙古电力") == "sys"

    def test_nan_inputs_do_not_crash(self):
        """Missing 供电局/所属地区 arrive as NaN floats from pandas."""
        import math
        assert self.z(channel="省内", trade_type="年度挂牌", consumer_unit="某用户",
                      region=math.nan, bureau=math.nan) == "west"


class TestCfdZones:
    def _intra(self, rows):
        cols = ["channel", "trade_type", "energy_kind", "consumer_unit", "region",
                "bureau", "volume_mwh", "energy_price", "env_value"]
        return pd.DataFrame(rows, columns=cols)

    def test_zone_weighted_beats_single_ref(self):
        """Two contracts same price, different zones → different CfD."""
        from services.wind_settlement.rules import cfd_zones

        intra = self._intra([
            ("省内", "年度挂牌", "正常", "东用户", "乌兰察布", "乌兰察布电业局", 1000.0, 250.0, 0.0),
            ("省内", "年度挂牌", "正常", "西用户", "乌海", "乌海电业局", 1000.0, 250.0, 0.0),
            ("省内", "年度挂牌", "汇总", None, None, None, 2000.0, 250.0, 0.0),
        ])
        # east ref 380, west ref 220 → 1000*(250-380) + 1000*(250-220) = -130000 + 30000
        out = cfd_zones(intra, pd.DataFrame(), ref_east=380.0, ref_west=220.0, ref_sys=300.0, unified=False)
        assert out == pytest.approx(-100000.0)

    def test_unified_month_uses_sys_for_all(self):
        from services.wind_settlement.rules import cfd_zones

        intra = self._intra([
            ("省内", "年度挂牌", "正常", "东用户", "乌兰察布", "", 1000.0, 250.0, 0.0),
            ("省内", "年度挂牌", "汇总", None, None, None, 1000.0, 250.0, 0.0),
        ])
        out = cfd_zones(intra, pd.DataFrame(), ref_east=380.0, ref_west=220.0, ref_sys=300.0, unified=True)
        assert out == pytest.approx(1000.0 * (250.0 - 300.0))

    def test_swap_scaling_to_summary_volume(self):
        """正常 rows net to 汇总 volume via uniform scale (置换 netting)."""
        from services.wind_settlement.rules import cfd_zones

        intra = self._intra([
            ("省内", "多年期绿电", "正常", "东用户", "乌兰察布", "", 20000.0, 251.4, 31.5),
            ("省内", "多年期绿电", "汇总", None, None, None, 18000.0, 251.4, 31.5),  # 置换 −2000
        ])
        out = cfd_zones(intra, pd.DataFrame(), ref_east=100.0, ref_west=80.0, ref_sys=90.0, unified=False)
        assert out == pytest.approx(18000.0 * (251.4 - 100.0))

    def test_cross_rows_included(self):
        from services.wind_settlement.rules import cfd_zones

        cross = self._intra([
            ("跨省", "蒙西送京", "正常", "bj-user", None, "", 15000.0, 409.169, 3.725),
        ])
        out = cfd_zones(pd.DataFrame(), cross, ref_east=380.0, ref_west=220.0, ref_sys=300.0, unified=False)
        assert out == pytest.approx(15000.0 * (409.169 - 220.0))

    def test_empty(self):
        from services.wind_settlement.rules import cfd_zones

        assert cfd_zones(pd.DataFrame(), pd.DataFrame(), 1.0, 1.0, 1.0, False) == 0.0
