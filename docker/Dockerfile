# Dockerfile for EVA.
#
# The SGE qsub program needs a proper hostname, and the SGE_MASTER variable.
#
# $ docker run --hostname=<HOSTNAME> \
#              --env=SGE_MASTER=<SGE_MASTER_HOSTNAME> \
#              --log-driver=syslog \
#              metno/eva

FROM ubuntu:14.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y --no-install-recommends install python-dev libzmq3-dev build-essential python-pip git gridengine-client
RUN git clone https://github.com/metno/eva.git /opt/eva
RUN pip install -e /opt/eva/
RUN mkdir -p /var/lib/eva
COPY start-eva /usr/bin/start-eva
ENTRYPOINT ["/usr/bin/start-eva"]
CMD python -m eva --config /opt/eva/eva-sample.ini
