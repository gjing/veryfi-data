FROM python:3.10-alpine
WORKDIR /code
ENV FLASK_APP=api.py
ENV FLASK_RUN_HOST=0.0.0.0
RUN apk add --no-cache gcc musl-dev linux-headers
RUN pip install poetry
COPY poetry.lock pyproject.toml ./
RUN poetry install
EXPOSE 5000
COPY . .
CMD ["poetry", "run",  "flask", "--app", "api", "run"]