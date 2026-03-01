FROM python:3.14-slim

# Install system dependencies required by psycopg2
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy everything needed for install
COPY pyproject.toml uv.lock ./
COPY src/ src/
COPY main.py .

# Install dependencies and the project itself
RUN uv sync --frozen --no-dev

# Run the ETL
CMD ["uv", "run", "python", "main.py"]
