FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY static/ ./static/
COPY templates/ ./templates/
COPY scripts/ ./scripts/

CMD ["python", "src/main.py"]
