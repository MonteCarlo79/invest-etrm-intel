# 新能源中长期签约比例要求 (MLT Contract Requirements)

省级新能源中长期签约比例（占本省新能源发电量 %）。供 Interconnector tab
S3/A2（新能源中长期义务与出口信号）使用：`load_mlt_rules` 读入本文件，
与 DB 覆盖（staging.interconnector_mlt_pct_override）及 80% 默认值合成
各省 MLT%（优先级：已覆盖 > 规则库 > 默认80%）。

## 加载格式 (loader format)

`services/interconnector/data.py::load_mlt_rules` 只解析如下行，其余行全部忽略：

    - 省份: NN%

- 省份为 2–4 个汉字（与通道注册表 send_prov 口径一致，如 蒙西、甘肃、黑龙江）。
- 冒号中英文均可（: 或 ：），行尾百分号必须保留。
- 注释、标题、空行不参与解析。

## 规则 (Rules)

（待 Task 17 填写 — 逐省一行，附来源引用与生效日期为注释。在填写并经用户
review 前，所有省份按 80% 默认值处理，UI 标记为「默认80%」。）
