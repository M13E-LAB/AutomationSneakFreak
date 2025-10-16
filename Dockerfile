ARG PYTHON_VERSION=3.11
ARG VARIANT=slim
FROM python:${PYTHON_VERSION}-${VARIANT}

RUN pip install kafka-python snowflake-connector-python python-dotenv pandas snowflake-connector-python[pandas] faker prometheus_client

WORKDIR /app
COPY . /app

CMD ["bash"]