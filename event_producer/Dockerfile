FROM python:3.10-slim

WORKDIR /app

RUN pip install kafka-python==2.3.1

CMD ["python", "produce_stream_event.py"]