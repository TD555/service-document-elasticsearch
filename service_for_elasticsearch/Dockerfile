FROM python:3.8.10

RUN adduser --disabled-login service-elasticsearch
USER service-elasticsearch

WORKDIR  /var/www/service_elasticsearch/service-for-elasticsearch 

COPY --chown=service-elasticsearch:service-elasticsearch .   /var/www/service_elasticsearch/service-for-elasticsearch 

COPY --chown=service-elasticsearch:service-elasticsearch ./requirements.txt  /var/www/service_elasticsearch/service-for-elasticsearch/requirements.txt
RUN python3 -m pip install --no-cache-dir  -r requirements.txt

CMD ["python3", "run.py"]
