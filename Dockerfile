FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --frozen

COPY src/ src/

ENTRYPOINT ["uv", "run", "tractatus", "catalog"]
CMD ["publish"]
