FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir PyYAML==6.0.3

COPY run_flow.py .

ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "/app/run_flow.py"]
