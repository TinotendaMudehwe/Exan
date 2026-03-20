FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install Python dependencies first for better layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source.
COPY . .

EXPOSE 5000

# Use gunicorn for production-style serving.
CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:${PORT:-5000} --workers 2 --threads 4 --timeout 120 app:app"]
