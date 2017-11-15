FROM jcollado/python-ci:0.2
RUN apk add --no-cache gcc libc-dev libffi-dev linux-headers openssl-dev python2-dev python3-dev
CMD ["/bin/sh"]
