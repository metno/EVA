# Dockerfile for EVA base. This Docker image contains FIMEX and the Python
# libraries required to build EVA.

FROM ubuntu:14.04
RUN useradd -M --system --uid 12325 eventadapter
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive \
    apt-get -y --no-install-recommends install \
        build-essential \
        git \
        libzmq3-dev \
        python3-dev \
        python3-pip \
        software-properties-common \
        wget \
    && add-apt-repository --yes ppa:heiko-klein/fimex \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive \
    apt-get -y install \
        fimex-bin \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /var/lib/eva && chown eventadapter /var/lib/eva
