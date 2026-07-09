FROM python:3.12-slim
COPY onefs_exporter.py /app/onefs_exporter.py
EXPOSE 9684
ENTRYPOINT ["python3", "/app/onefs_exporter.py"]
