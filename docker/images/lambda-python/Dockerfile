FROM python:3.7-stretch
ARG reqs=dev-

RUN set -eux; \
  apt-get update; \
  apt-get install -y gosu; \
  rm -rf /var/lib/apt/lists/*;

COPY ${reqs}requirements.txt .
RUN pip install --upgrade pip pipenv && \
pip install -r ${reqs}requirements.txt

COPY docker/files/entrypoint.sh /usr/local/bin
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /usr/src/app
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"] 

