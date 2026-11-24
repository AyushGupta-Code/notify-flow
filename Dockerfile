# One image, several processes (api, relay, consumer, worker); the command picks the role.
FROM python:3.12-slim AS build
WORKDIR /app
RUN pip install --no-cache-dir --upgrade pip
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir --prefix=/install .

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
RUN useradd --create-home --uid 10001 app
COPY --from=build /install /usr/local
COPY alembic.ini ./
COPY migrations ./migrations
COPY prompts ./prompts
COPY models ./models
COPY src ./src
ENV NF_PROMPT_REGISTRY_PATH=/app/prompts/registry.yaml \
    NF_CHANNEL_MODEL_PATH=/app/models/channel_lr_v1.json
USER 10001
EXPOSE 8000 9101
CMD ["uvicorn", "notify_flow.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
