"""调用 Claude（Anthropic Messages API）。直接用 HTTP，不依赖任何 SDK"""

import json
import time
import urllib.error
import urllib.request

from common import log
from copilot import config

SERVICE = "copilot"


class LLMError(RuntimeError):
    pass


class Usage:
    def __init__(self):
        self.input_tokens = 0
        self.output_tokens = 0
        self.calls = 0

    def add(self, u):
        self.calls += 1
        self.input_tokens += int(u.get("input_tokens", 0)) + int(u.get("cache_read_input_tokens", 0) or 0) \
            + int(u.get("cache_creation_input_tokens", 0) or 0)
        self.output_tokens += int(u.get("output_tokens", 0))

    @property
    def cost_usd(self):
        return round(self.input_tokens / 1e6 * config.PRICE_IN + self.output_tokens / 1e6 * config.PRICE_OUT, 5)


def enabled():
    return bool(config.API_KEY)


def call(system, messages, tools=None, tool_choice=None, usage=None, max_tokens=None):
    """发一次请求，返回 Claude 的回复（dict：content、stop_reason、usage）。遇到限流 / 服务端错误会重试"""
    body = {"model": config.MODEL, "max_tokens": max_tokens or config.MAX_TOKENS, "system": system, "messages": messages}
    if tools:
        body["tools"] = tools
    if tool_choice:
        body["tool_choice"] = tool_choice
    data = json.dumps(body, ensure_ascii=False).encode()
    last = None
    for attempt in range(4):
        req = urllib.request.Request(f"{config.API_URL}/v1/messages", data=data, method="POST", headers={
            "x-api-key": config.API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                resp = json.load(r)
            if usage is not None:
                usage.add(resp.get("usage", {}))
            return resp
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")[:500]
            last = f"HTTP {e.code}: {detail}"
            if e.code in (429, 500, 502, 503, 504, 529):
                log(SERVICE, "WARN", "llm_retry", attempt=attempt + 1, error=last)
                time.sleep(2 ** attempt * 3)
                continue
            raise LLMError(last) from None
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last = f"{type(e).__name__}: {e}"
            log(SERVICE, "WARN", "llm_retry", attempt=attempt + 1, error=last)
            time.sleep(2 ** attempt * 3)
    raise LLMError(last or "unknown error")


def tool_uses(resp):
    return [b for b in resp.get("content", []) if b.get("type") == "tool_use"]


def text_of(resp):
    return "\n".join(b.get("text", "") for b in resp.get("content", []) if b.get("type") == "text").strip()
