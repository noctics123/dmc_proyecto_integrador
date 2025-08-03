# docker/Dockerfile
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PORT=8080

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY landing_run/ ./landing_run/

CMD ["uvicorn", "landing_run.main:app", "--host", "0.0.0.0", "--port", "8080"]
