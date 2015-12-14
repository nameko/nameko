# to build this container:
# docker build -t crossbar .
FROM ubuntu:trusty

RUN apt-get update && apt-get -y install build-essential libssl-dev libffi-dev libreadline-dev libbz2-dev libsqlite3-dev libncurses5-dev python-pip python-dev
RUN pip install crossbar[all]

# This creates the default "hello world" application.
RUN crossbar init --template hello:python --appdir /hello

# You need to forward port 8080 with "-p 8080" on the command-line:
# docker run -p 8080 crossbar
ENTRYPOINT ["crossbar", "start", "--cbdir", "/hello/.crossbar"]