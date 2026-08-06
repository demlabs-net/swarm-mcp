FROM python:3.12-slim

WORKDIR /app

# agent-dev-0 reaches the Aliyun mirror reliably; callers can override this
# build argument for another trusted package mirror.
ARG PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple
ENV PIP_INDEX_URL=${PIP_INDEX_URL}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

ENV PYTHONPATH=/app/src
EXPOSE 3004

CMD ["python", "-m", "swarm_mcp.server"]
