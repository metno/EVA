# Dockerfile for EVA.
#
# Configuration environment variables:
#
# EVA_LOG_CONFIG
# EVA_PRODUCTSTATUS_URL
# EVA_PRODUCTSTATUS_USERNAME
# EVA_PRODUCTSTATUS_API_KEY
# EVA_PRODUCTSTATUS_EVENT_SOCKET
# EVA_PRODUCTSTATUS_VERIFY_SSL
# EVA_ADAPTER
# EVA_EXECUTOR
# EVA_CHECKPOINT_FILE

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
RUN useradd -M --system --uid 12325 eventadapter
RUN mkdir -p /var/lib/eva && chown eventadapter /var/lib/eva
COPY . /opt/eva
#RUN git clone https://github.com/metno/eva.git /opt/eva
RUN pip install -e /opt/eva/
ENV EVA_LOG_CONFIG /opt/eva/etc/logging.ini
CMD python -m eva
