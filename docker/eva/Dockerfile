# Dockerfile for EVA.

FROM metno/eva-base
RUN git clone --depth 1 https://gitlab.met.no/modellprod/eva-mirror.git /opt/eva
RUN git clone --depth 1 https://gitlab.met.no/it-geo/eva-adapter-support.git /opt/eva-adapter-support
RUN pip3 install -e /opt/eva/
USER eventadapter
ENV EVA_LOG_CONFIG /opt/eva/etc/logging.ini
CMD python3 -m eva
