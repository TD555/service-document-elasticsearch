FROM python:3.8.10

# RUN rm -rf /var/lib/apt/lists/* &&  apt-get update && \
#     apt-get install -yf unoconv libreoffice --no-install-recommends

# RUN cp /usr/bin/unoconv /usr/local/bin/unoconv

# RUN sed -i 's|#!/usr/bin/env python3|#!/usr/bin/python3|' /usr/local/bin/unoconv

RUN adduser --disabled-login service-elasticsearch
USER service-elasticsearch

WORKDIR  /var/www/service_elasticsearch/service-for-elasticsearch 

COPY --chown=service-elasticsearch:service-elasticsearch .   /var/www/service_elasticsearch/service-for-elasticsearch 

COPY --chown=service-elasticsearch:service-elasticsearch ./requirements.txt  /var/www/service_elasticsearch/service-for-elasticsearch/requirements.txt

ENV PATH="$PATH:/home/service-elasticsearch/.local/bin"

RUN python3 -m pip install --no-cache-dir  -r requirements.txt

CMD ["python3", "run.py"]
