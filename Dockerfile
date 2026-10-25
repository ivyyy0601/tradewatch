# TradeWatch 自己的程序（engine、monitor、tw）用的镜像
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 TZ=America/New_York

COPY requirements-server.txt .
RUN pip install --no-cache-dir -r requirements-server.txt

COPY *.py ./
