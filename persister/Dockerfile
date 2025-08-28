FROM python:3.12-slim

WORKDIR /app

COPY /persister/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY /persister /app/persister

COPY /kafka_objects /app/kafka_objects

COPY /DAL /app/DAL

COPY /connection /app/connection

CMD ["python", "persister/main.py"]

