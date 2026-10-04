from .io import load_yaml


def load_glossary(path) -> list:
    return load_yaml(path)["terms"]


def check_pair(en_body: str, zh_body: str, terms: list) -> list:
    low = en_body.lower()
    return [f"'{t['en']}' should appear as '{t['zh']}'"
            for t in terms if t["en"].lower() in low and t["zh"] not in zh_body]


def prompt_block(terms: list) -> str:
    lines = "\n".join(f"- {t['en']} = {t['zh']}" for t in terms)
    return "Use these approved EN=ZH terms exactly:\n" + lines
