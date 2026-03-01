FROM python:3.14-slim

# Install system dependencies required by psycopg2
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first (layer caching)
COPY pyproject.toml uv.lock ./

# Install production dependencies only
RUN uv sync --frozen --no-dev

# Copy source code
COPY src/ src/
COPY main.py .

# Install the package itself
RUN uv pip install -e .

# Run the ETL
CMD ["uv", "run", "python", "main.py"]
