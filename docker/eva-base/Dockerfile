# Dockerfile for EVA base. This Docker image contains FIMEX and the Python
# libraries required to build EVA.

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
