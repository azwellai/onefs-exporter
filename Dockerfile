FROM python:3.12-slim
COPY onefs_exporter.py /app/onefs_exporter.py
RUN useradd --system --no-create-home --uid 65532 exporter
USER exporter
EXPOSE 9684
ENTRYPOINT ["python3", "/app/onefs_exporter.py"]
