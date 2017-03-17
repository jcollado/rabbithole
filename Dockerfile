FROM alpine:3.5
RUN apk add --no-cache git openssh tar python python3 && \
    pip3 install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir tox
