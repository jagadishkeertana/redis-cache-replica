FROM python:3.11-slim

WORKDIR /app

# Copy project files
COPY server/ /app/server/
COPY cluster/ /app/cluster/
COPY client/ /app/client/

# Create data directory for WAL files
RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["python", "-m", "cluster.router"]
