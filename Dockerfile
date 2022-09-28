FROM python:3.10-buster

WORKDIR /app

RUN git clone https://github.com/mkeras/sparkplug-b-dataclasses

RUN mv ./sparkplug-b-dataclasses ./sparkplug_b

COPY ./src/requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY ./src /app

CMD python run.py