FROM python:3.14-slim

ENV TZ=Asia/Kolkata \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    COMPOSE_BAKE=true \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_HTTP_TIMEOUT=90 \
    UV_NO_PROGRESS=1 \
    UV_CONCURRENT_DOWNLOADS=20 \
    PATH="/app/.venv/bin:$PATH" \
    IPYTHONDIR=/app/.ipython

# Install curl first (needed for uv installation)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tzdata \
        curl \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/share/zoneinfo/Asia/Kolkata /etc/localtime \
    && echo "Asia/Kolkata" > /etc/timezone

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/ && \
    mv /root/.local/bin/uvx /usr/local/bin/

WORKDIR /app

COPY ./pyproject.toml uv.lock ./
COPY modules/ipython_startup.py /app/modules/ipython_startup.py

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

RUN echo 'alias ipython="uv run ipython"' >> /root/.bashrc && \
    echo 'alias cls="clear"' >> /root/.bashrc

RUN mkdir -p /app/.ipython/profile_default/startup/ && \
    cp /app/modules/ipython_startup.py /app/.ipython/profile_default/startup/auto_reload.py