# One image for every Python service; docker-compose.prod.yaml picks the command for each.
# The base image and all pinned requirements have ARM builds, so this works on the Oracle ARM VM.
FROM python:3.11.14-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY *.py schema.sql ./
COPY scripts ./scripts
COPY static ./static
COPY replays ./replays

# don't run as root
RUN useradd --create-home app && chown -R app /app
USER app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
