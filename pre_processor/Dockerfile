FROM python:3.12-slim

WORKDIR /app

COPY /pre_processor/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY /pre_processor /app/pre_processor

COPY /kafka_objects /app/kafka_objects

CMD ["python", "pre_processor/main.py"]