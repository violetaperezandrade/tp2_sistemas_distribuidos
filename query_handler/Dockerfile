FROM python:3.11-slim
COPY query_handler /
COPY util /util
COPY client /client
COPY requirements.txt /
RUN pip install pip --upgrade
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT ["/bin/sh"]