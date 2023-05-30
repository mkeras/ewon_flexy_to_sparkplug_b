FROM python:3.9-buster

WORKDIR /app

COPY ./src/requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY ./src /app

CMD python run.py