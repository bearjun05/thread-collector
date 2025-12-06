FROM python:3.11-slim

WORKDIR /app

# 시스템 의존성 설치
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# uv 설치
RUN pip install --no-cache-dir uv

# 프로젝트 의존성 복사 및 설치
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

# Playwright 브라우저 설치
RUN uv run playwright install chromium
RUN uv run playwright install-deps chromium

# 애플리케이션 코드 복사
COPY . .

EXPOSE 8000

# 서버 실행
CMD ["uv", "run", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

