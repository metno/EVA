# Dockerfile for EVA.

FROM metno/eva-base
RUN git clone https://github.com/metno/eva.git /opt/eva
RUN git clone https://gitlab.met.no/it-geo/eva-adapter-support.git /opt/eva-adapter-support
RUN pip install -e /opt/eva/
USER eventadapter
ENV EVA_LOG_CONFIG /opt/eva/etc/logging.ini
CMD python -m eva
