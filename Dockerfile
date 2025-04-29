FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=3.0.0
ENV PYTHONPATH=/opt/airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"

# Create airflow user and directories
RUN useradd -ms /bin/bash airflow && \
    mkdir -p /opt/airflow && \
    chown -R airflow:airflow /opt/airflow

# Switch to airflow user
USER airflow

# Install Airflow and its dependencies
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow[celery]==${AIRFLOW_VERSION}" \
    "apache-airflow[postgres]==${AIRFLOW_VERSION}"

# Copy requirements and package
COPY --chown=airflow:airflow requirements.txt ${AIRFLOW_HOME}/requirements.txt
COPY --chown=airflow:airflow datafin-package ${AIRFLOW_HOME}/datafin-package

# Install project dependencies
WORKDIR ${AIRFLOW_HOME}
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -e ./datafin-package

# Expose ports
EXPOSE 8080

# Set entrypoint
ENTRYPOINT ["airflow"] 