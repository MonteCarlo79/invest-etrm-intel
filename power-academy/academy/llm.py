import json

USAGE = {"in": 0, "out": 0}


def parse_json(text: str) -> dict:
    i, j = text.find("{"), text.rfind("}")
    if i == -1 or j <= i:
        raise ValueError("no json object")
    return json.loads(text[i:j + 1])


def call_json(client, model, system, user, max_tokens=2000, retries=1) -> dict:
    last = None
    for _ in range(retries + 1):
        resp = client.messages.create(
            model=model, max_tokens=max_tokens, system=system,
            messages=[{"role": "user", "content": user}])
        usage = getattr(resp, "usage", None)
        if usage is not None:
            USAGE["in"] += getattr(usage, "input_tokens", 0)
            USAGE["out"] += getattr(usage, "output_tokens", 0)
        try:
            return parse_json(resp.content[0].text)
        except (ValueError, json.JSONDecodeError) as e:
            last = e
            import logging
            logging.getLogger("llm").warning(
                "call_json parse failure (attempt %d): %s | raw[:400] = %r",
                _ + 1, e, resp.content[0].text[:400])
    raise ValueError(f"unparseable LLM output: {last}")
