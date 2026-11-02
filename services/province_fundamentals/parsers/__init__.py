# services/province_fundamentals/parsers/__init__.py
"""确定性月报解析器包 — 每省/每格式一个模块 (结算单 parser_guangxi 同款套路).

Parser 鸭子类型 (registry 依赖):
    FORMAT_NAME: str                          — e.g. "format_a"
    sniff(path) -> bool                       — 结构嗅探, 永不看文件名
    parse(path, province) -> list[FundRow]
"""
