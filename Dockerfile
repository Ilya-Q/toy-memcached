FROM python

WORKDIR /toy-memcached
COPY . /toy-memcached

ENTRYPOINT ["python", "node.py"]

# docker run -it --rm toy-memcached --loglevel DEBUG
# docker build -t toy-memcached .
# docker run --rm -it --entrypoint python toy-memcached cli.py