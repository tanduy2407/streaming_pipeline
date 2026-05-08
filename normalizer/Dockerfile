FROM python:3.10-slim
WORKDIR /app
RUN pip install kafka-python==2.3.1 opensearch-py==3.2.0
COPY . .
ENV PYTHONPATH=/app
RUN chmod +x /app/main/start.sh
CMD ["/app/main/start.sh"]