FROM python:3.12-slim

WORKDIR /app

COPY /enricher/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY /enricher /app/enricher

COPY /kafka_objects /app/kafka_objects

CMD ["python", "enricher/main.py"]

