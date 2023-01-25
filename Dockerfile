FROM python

WORKDIR /toy-memcached
COPY . /toy-memcached

ENTRYPOINT ["python", "node.py"]