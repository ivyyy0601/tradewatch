"""Telegram：发调查结果；带"批准 / 拒绝"按钮；收按钮点击（只接受配置的那个聊天）"""

import json
import urllib.parse
import urllib.request

from common import log
from copilot import config

SERVICE = "copilot"


def enabled():
    return bool(config.TG_TOKEN and config.TG_CHAT)


def _api(method, payload, timeout=10):
    data = urllib.parse.urlencode({k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
                                   for k, v in payload.items()}).encode()
    with urllib.request.urlopen(f"https://api.telegram.org/bot{config.TG_TOKEN}/{method}", data=data,
                                timeout=timeout) as r:
        return json.load(r)


def send(text, buttons=None):
    """buttons: [[(文字, callback_data), ...], ...]；返回 message_id"""
    if not enabled():
        return None
    payload = {"chat_id": config.TG_CHAT, "text": text[:4000], "disable_web_page_preview": "true"}
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": [[{"text": t, "callback_data": d} for t, d in row] for row in buttons]}
    try:
        return _api("sendMessage", payload).get("result", {}).get("message_id")
    except Exception as e:                                       # noqa: BLE001
        log(SERVICE, "WARN", "telegram_failed", error=str(e)[:200])
        return None


def edit(message_id, text):
    if not enabled() or not message_id:
        return
    try:
        _api("editMessageText", {"chat_id": config.TG_CHAT, "message_id": message_id, "text": text[:4000]})
    except Exception as e:                                       # noqa: BLE001
        log(SERVICE, "WARN", "telegram_edit_failed", error=str(e)[:200])


def answer(callback_id, text):
    try:
        _api("answerCallbackQuery", {"callback_query_id": callback_id, "text": text[:180]})
    except Exception:                                            # noqa: BLE001
        pass


def updates(offset, timeout=25):
    """长轮询拿按钮点击。返回 (新 offset, [(callback_id, data, 谁点的, message_id)])"""
    r = _api("getUpdates", {"offset": offset, "timeout": timeout, "allowed_updates": ["callback_query"]},
             timeout=timeout + 10)
    out = []
    for u in r.get("result", []):
        offset = max(offset, u["update_id"] + 1)
        cq = u.get("callback_query")
        if not cq:
            continue
        chat_id = str(cq.get("message", {}).get("chat", {}).get("id", ""))
        if chat_id != str(config.TG_CHAT):                       # 只接受我们自己的聊天
            answer(cq["id"], "not allowed")
            continue
        user = cq.get("from", {})
        who = user.get("username") or user.get("first_name") or str(user.get("id"))
        out.append((cq["id"], cq.get("data", ""), f"telegram:{who}", cq.get("message", {}).get("message_id")))
    return offset, out
