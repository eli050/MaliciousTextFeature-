FROM python:3.13-slim

WORKDIR /app


COPY /retriever/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY /retriever /app/retriever

COPY /kafka_objects /app/kafka_objects

CMD ["python", "retriever/main.py"]
