FROM python:3.11.3-slim

RUN mkdir logzio
WORKDIR logzio
COPY requirements.txt .
COPY src ./src
RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "-m", "src.main"]