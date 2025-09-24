FROM python:3.11

RUN apt-get update && apt-get install -y \
bash \
wget \
curl \
make \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-trino dbt-core

WORKDIR /usr/app