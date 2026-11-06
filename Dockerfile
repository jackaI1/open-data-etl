FROM python:3.12-slim-bookworm AS batch
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir '.[gcp]' && useradd --create-home runner
ENV SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON=python PYTHONUNBUFFERED=1
USER runner
ENTRYPOINT ["python", "-m", "dpd_etl.gcp_batch"]

FROM batch AS report
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["exec gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 1 --threads 4 'dpd_etl.report_service:create_app()'"]
