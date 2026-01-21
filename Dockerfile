FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

# Copy source code
COPY src/ src/
COPY examples/ examples/

# Set entrypoint
ENTRYPOINT ["dbt-datahub-governance"]
CMD ["--help"]
