FROM apache/airflow:3.1.6

USER root
# Copy uv from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Ensure we switch back to the airflow user for installation
USER airflow

# Copy dependency files
COPY requirements.txt /requirements.txt
COPY pyproject.toml uv.lock ./

# Install dependencies using uv into the airflow environment
RUN uv pip install --no-cache-dir -r /requirements.txt
