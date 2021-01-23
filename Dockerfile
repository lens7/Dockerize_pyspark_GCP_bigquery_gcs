FROM python:3.7-stretch
COPY . /app
WORKDIR /app

RUN apt-get update
RUN apt-get install default-jdk -y
RUN pip install -r requirements.txt

CMD python app4.py
