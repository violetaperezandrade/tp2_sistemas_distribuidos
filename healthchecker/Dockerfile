FROM python:3.11-alpine
RUN apk update
RUN apk add docker
COPY healthchecker /
COPY client /client
COPY util /util
COPY requirements.txt /
RUN pip install pip --upgrade
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT ["/bin/sh"]