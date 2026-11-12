FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir PyYAML==6.0.3 pymssql==2.4.3 oracledb==26.0.1

COPY run_flow.py source_ddl.py ./

ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "/app/run_flow.py"]
