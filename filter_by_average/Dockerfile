FROM python:3.11-slim
COPY filter_by_average /
COPY util /util
COPY client /client
COPY requirements.txt /
RUN pip install pip --upgrade
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT ["/bin/sh"]