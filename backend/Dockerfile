FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY backend/requirements.txt /tmp/backend-requirements.txt
RUN pip install -r /tmp/backend-requirements.txt

COPY . /app

RUN mkdir -p /app/backend/data /app/runtime

EXPOSE 8000

WORKDIR /app/runtime

CMD ["python", "-m", "uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
