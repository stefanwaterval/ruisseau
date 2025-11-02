FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /ruisseau

COPY . .

RUN pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "ruisseau.cli"]
