# Dockerfile for EVA.
# See the file eva/__main__.py for supported configuration variables.

FROM ubuntu:14.04
RUN useradd -M --system --uid 12325 eventadapter
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive \
    apt-get -y --no-install-recommends install \
        python-dev \
        libzmq3-dev \
        build-essential \
        python-pip \
        git \
        wget \
        software-properties-common \
    && add-apt-repository --yes ppa:heiko-klein/fimex \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive \
    apt-get -y install \
        fimex-bin \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /var/lib/eva && chown eventadapter /var/lib/eva
RUN git clone https://github.com/metno/eva.git /opt/eva
RUN git clone https://gitlab.met.no/it-geo/eva-adapter-support.git /opt/eva-adapter-support
RUN pip install -e /opt/eva/
USER eventadapter
ENV EVA_LOG_CONFIG /opt/eva/etc/logging.ini
CMD python -m eva
