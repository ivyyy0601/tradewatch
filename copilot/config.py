"""copilot 的配置：全部从环境变量读（服务器上的 .env），改配置不用改代码"""

import os

HERE = os.path.dirname(os.path.abspath(__file__))

# ---- AI（不填 ANTHROPIC_API_KEY 也能跑：只用规则判断，不调用 AI）----
API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
API_URL = os.getenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com").rstrip("/")
MODEL = os.getenv("COPILOT_MODEL", "claude-sonnet-5-5")
MAX_STEPS = int(os.getenv("COPILOT_MAX_STEPS", "8"))              # 一次调查最多调用几次工具
MAX_TOKENS = int(os.getenv("COPILOT_MAX_TOKENS", "4000"))
PRICE_IN = float(os.getenv("COPILOT_PRICE_IN", "3"))              # 每百万 input token 多少美元（用来估算费用）
PRICE_OUT = float(os.getenv("COPILOT_PRICE_OUT", "15"))
DAILY_BUDGET_USD = float(os.getenv("COPILOT_DAILY_BUDGET_USD", "3"))   # 每天最多花多少，超了就只用规则判断

# ---- 流水线 ----
CORRELATE_WINDOW_MIN = int(os.getenv("COPILOT_CORRELATE_WINDOW_MIN", "10"))   # 多久内的相关告警算同一个事故
MAX_INCIDENT_SPAN_MIN = int(os.getenv("COPILOT_MAX_INCIDENT_SPAN_MIN", "45"))   # 一个事故最长多久，超过就算新事故（防止一串告警无限连下去）
DEBOUNCE_S = int(os.getenv("COPILOT_DEBOUNCE_S", "60"))         # 事故开始后等多久再调查，让相关告警先归进来
MAX_AGE_MIN = int(os.getenv("COPILOT_MAX_AGE_MIN", "60"))       # 超过这么久的老告警不自动调查（可以手动）
AUTONOMY = os.getenv("COPILOT_AUTONOMY", "suggest")             # read = 只调查；suggest = 可以建议操作（要人批准）
PROPOSAL_TTL_MIN = int(os.getenv("COPILOT_PROPOSAL_TTL_MIN", "30"))   # 建议多久没人批准就作废

# ---- 外部系统 ----
LOKI_URL = os.getenv("LOKI_URL", "http://loki:3100").rstrip("/")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
ANSWERS = os.getenv("CHAOS_ANSWERS", "data/answers.jsonl")
DOCS_DIR = os.getenv("COPILOT_DOCS_DIR", "docs")

TOOL_RESULT_CHARS = 6000        # 每次工具结果最多给 AI 多少字符
EVIDENCE_CHARS = 24000          # 证据包最多多少字符
QUERY_MAX_ROWS = 60
QUERY_TIMEOUT_MS = 5000
