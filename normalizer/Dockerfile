FROM python:3.10-slim

WORKDIR /app

RUN pip install kafka-python==2.3.1
ENV PYTHONPATH=/app

CMD ["python", "main/normalized.py"]