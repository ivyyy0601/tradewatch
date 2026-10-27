"""
各个服务共用的小工具：写日志、连数据库、收发 Kafka 消息、记录订单事件
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2

from config import BROKERS

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# 所有 Kafka topic（"频道"）。每个服务只往自己负责的频道写，从上游频道读
TOPICS = [
    "md.exchange",        # 真实市场行情（只有模拟券商用：券商按真实价格撮合）
    "md.bars",            # 我们收到的行情（我们的系统用；行情源出问题时这里会停住）
    "orders.events",      # 每张订单发生的每件事（母单、风控、子单、成交……）
    "orders.parent",      # 通过风控的母单 → 交给 EMS 执行
    "exec.orders",        # EMS 要发的子单 / 撤单 → 交给 FIX 网关
    "exec.reports",       # FIX 网关翻译好的券商回报 → 交给 EMS
    *[f"fix.to.{b}" for b in BROKERS],     # 发给券商的 FIX 原文
    *[f"fix.from.{b}" for b in BROKERS],   # 券商发来的 FIX 原文
    "fix.log",            # 收发的每条 FIX 消息（存档用）
    "ops.commands",       # 值班操作：暂停 / 恢复 PM、撤母单（tw 命令行和监控发的）
    "ops.state",          # 状态更新：借券额度、PM 开关、FIX 连接、服务心跳
    "ops.alerts",         # 告警事件：monitor 每触发 / 解决一条告警就发一条（AI 值班助手 copilot 读）
]


def now():
    return datetime.now(timezone.utc)


def new_id(prefix, n=8):
    return prefix + uuid.uuid4().hex[:n].upper()


# ============================================================
# 日志：每行一条 JSON
# ============================================================
def log(service, level, msg, **fields):
    """标准格式日志：每行一条 JSON，方便以后搜索和统计。
    例：{"ts": "...", "level": "WARN", "service": "risk", "msg": "RISK_REJECT", "parent_id": "..."}"""
    if os.getenv("TW_QUIET") and level == "INFO":      # 命令行工具不需要这些日志
        return
    record = {
        "ts": now().isoformat(timespec="milliseconds"),
        "level": level,
        "service": service,
        "msg": msg,
        **{k: v for k, v in fields.items() if v is not None},
    }
    print(json.dumps(record, ensure_ascii=False, default=str), flush=True)


# ============================================================
# 数据库
# ============================================================
def connect(service):
    """连接数据库。数据库刚启动时可能还没准备好，所以失败了会重试。"""
    for attempt in range(1, 61):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "postgres"),   # docker compose 里数据库的服务名
                port=int(os.getenv("DB_PORT", "5432")),
                dbname="tradewatch",
                user="tradewatch",
                password=os.environ["POSTGRES_PASSWORD"],
            )
            conn.autocommit = True                       # 每条 SQL 执行完立刻生效
            log(service, "INFO", "db_connected")
            return conn
        except psycopg2.OperationalError as e:
            log(service, "WARN", "db_not_ready", attempt=attempt, error=str(e).strip())
            time.sleep(2)
    raise SystemExit("cannot connect to database")


# ============================================================
# Kafka
# ============================================================
from confluent_kafka import OFFSET_END, Consumer, KafkaError, Producer  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402


def ensure_topics(service):
    """等 Kafka 启动好，并确保所有 topic 都已经建好。"""
    admin = AdminClient({"bootstrap.servers": KAFKA})
    for attempt in range(1, 91):
        try:
            existing = set(admin.list_topics(timeout=5).topics)
            break
        except Exception as e:                             # noqa: BLE001
            log(service, "WARN", "kafka_not_ready", attempt=attempt, error=str(e))
            time.sleep(2)
    else:
        raise SystemExit("cannot connect to kafka")
    missing = [t for t in TOPICS if t not in existing]
    if missing:
        futures = admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in missing])
        for t, f in futures.items():
            try:
                f.result()
            except Exception as e:                         # noqa: BLE001  已经被别的服务建好了
                if "TOPIC_ALREADY_EXISTS" not in str(e) and "already exists" not in str(e):
                    raise
    log(service, "INFO", "kafka_connected", bootstrap=KAFKA)


class Bus:
    """往 Kafka 发消息。value 是字典就转成 JSON；FIX 原文直接发字符串。"""

    def __init__(self, service):
        self.service = service
        self.p = Producer({"bootstrap.servers": KAFKA, "client.id": service, "linger.ms": 5,
                           "enable.idempotence": True})

    def send(self, topic, key, value):
        data = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
        while True:
            try:
                self.p.produce(topic, key=str(key) if key is not None else None, value=data.encode())
                break
            except BufferError:                            # 本地发送队列满了，等一下
                self.p.poll(0.5)
        self.p.poll(0)

    def flush(self):
        self.p.flush(10)


def consumer(service, topics, from_end=False, group=None, auto_commit=True):
    """订阅 topic。
    from_end=True：每次启动都从最新的消息开始读（OMS、EMS 这种实时服务，重启后从数据库恢复状态，不重放旧消息）
    from_end=False：从上次读到的位置接着读（order-store、FIX 网关、券商：一条都不能漏）"""
    conf = {"bootstrap.servers": KAFKA, "group.id": group or service, "client.id": service,
            "auto.offset.reset": "latest" if from_end else "earliest",
            "enable.auto.commit": auto_commit}
    c = Consumer(conf)

    def jump_to_end(cons, partitions):
        for p in partitions:
            p.offset = OFFSET_END
        cons.assign(partitions)

    if from_end:
        c.subscribe(topics, on_assign=jump_to_end)
    else:
        c.subscribe(topics)
    return c


def poll(c, timeout=1.0, service="?"):
    """读一条消息，返回 (topic, key, value)；没有消息返回 None。JSON 自动转成字典。"""
    m = c.poll(timeout)
    if m is None:
        return None
    if m.error():
        if m.error().code() != KafkaError._PARTITION_EOF:
            log(service, "WARN", "kafka_error", error=str(m.error()))
        return None
    raw = m.value().decode() if m.value() is not None else ""
    key = m.key().decode() if m.key() is not None else None
    try:
        value = json.loads(raw) if raw[:1] in "{[" else raw
    except json.JSONDecodeError:
        log(service, "ERROR", "bad_message", topic=m.topic(), offset=m.offset(), raw=raw[:200])
        return None
    return m.topic(), key, value


def heartbeat(bus, service, **detail):
    bus.send("ops.state", service, {"kind": "heartbeat", "service": service, "ts": now(),
                                    "detail": json.dumps(detail, default=str) if detail else None})


def emit(bus, service, event_type, parent_id, child_id=None, detail=None, level="INFO",
         parent_new=None, parent=None, child_new=None, child=None, execution=None, **fields):
    """订单发生了一件事：发到 orders.events（order-store 会存进数据库），同时打一行日志。"""
    msg = {"eid": uuid.uuid4().hex, "ts": now(), "service": service, "type": event_type,
           "parent_id": parent_id, "child_id": child_id, "detail": detail}
    for k, v in (("parent_new", parent_new), ("parent", parent), ("child_new", child_new),
                 ("child", child), ("execution", execution)):
        if v is not None:
            msg[k] = v
    bus.send("orders.events", parent_id, msg)
    log(service, level, event_type, parent_id=parent_id, child_id=child_id, detail=detail, **fields)


def is_stale(msg, max_age_s=150):
    """行情消息太旧就跳过（服务重启或者积压后，不要拿旧价格做决定）。"""
    try:
        return (now() - datetime.fromisoformat(msg["ts"])).total_seconds() > max_age_s
    except (KeyError, ValueError):
        return False
