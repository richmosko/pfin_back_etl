FROM python:3.14-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy everything needed for install
COPY pyproject.toml uv.lock ./
COPY src/ src/
COPY main.py .
COPY mini.py .

# Install dependencies and the project itself
RUN uv sync --frozen --no-dev && uv pip install -e .
RUN apt-get update && apt-get install -y vim && rm -rf /var/lib/apt/lists/*

# Keep the container running (ETL is triggered via scheduled task)
CMD ["tail", "-f", "/dev/null"]
