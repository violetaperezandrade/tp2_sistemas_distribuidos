FROM python:3.11-slim
COPY client /
COPY util /util
COPY requirements.txt /
RUN pip install pip --upgrade
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir /results
ENTRYPOINT ["/bin/sh"]