FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml ./
COPY pnl ./pnl

RUN pip install --no-cache-dir -e ".[dev]"

EXPOSE 8000

CMD ["uvicorn", "pnl.main:app", "--host", "0.0.0.0", "--port", "8000"]
