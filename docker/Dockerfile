# Dockerfile for EVA.
#
# The SGE qsub program needs a proper hostname, and the SGE_MASTER variable.
#
# $ docker run --hostname=<HOSTNAME> \
#              --env=SGE_MASTER=<SGE_MASTER_HOSTNAME> \
#              --log-driver=syslog \
#              metno/eva

FROM ubuntu:14.04
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get -y --no-install-recommends install \
    python-dev \
    libzmq3-dev \
    build-essential \
    python-pip \
    git \
    gridengine-client \
    && apt-get clean
RUN git clone https://github.com/metno/eva.git /opt/eva
RUN pip install -e /opt/eva/
RUN useradd -M --system --uid 12325 eventadapter
RUN mkdir -p /var/lib/eva && chown eventadapter /var/lib/eva
CMD python -m eva \
    --log-config /opt/eva/etc/logging.ini \
    --productstatus-url https://productstatus.met.no \
    --productstatus-event-socket tcp://productstatus.met.no:9797
