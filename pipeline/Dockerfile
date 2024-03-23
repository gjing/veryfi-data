FROM python:3.10-alpine
WORKDIR /pipeline
RUN apk add --no-cache gcc musl-dev linux-headers
RUN pip install poetry
COPY poetry.lock pyproject.toml ./
RUN poetry install
EXPOSE 3001
COPY . .
ENTRYPOINT ["poetry", "run", "dagster-webserver", "-h", "0.0.0.0", "-p", "3001"]
