# Base Python image
FROM python:3.11-slim

# Prevent Python from writing pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy all project files
COPY . .

# Cloud Run default port
ENV PORT=8080
EXPOSE 8080

# SERVICE selects which Flask app this container serves, matching the
# module name directly:
#   SERVICE=app    -> app.py    (public-facing upload/poll/download API)
#   SERVICE=worker -> worker.py (Cloud Tasks target, processes one batch per request)
ENV SERVICE=app

ENTRYPOINT ["/bin/sh", "-c", "gunicorn -b 0.0.0.0:${PORT} ${SERVICE}:app --timeout 600 --workers 1"]
