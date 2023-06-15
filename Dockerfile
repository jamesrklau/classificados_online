FROM python:3.9

RUN apt-get update && apt-get install -y firefox-esr

WORKDIR /code

COPY ./requirements.txt  ./requirements.txt

RUN pip install --no-cache-dir --upgrade -r ./requirements.txt

COPY src src