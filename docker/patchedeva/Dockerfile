# Dockerfile for EVA.

FROM metno/eva:4.3.0

USER root
ADD https://raw.githubusercontent.com/metno/EVA/master/eva/executor/grid_engine.py /opt/eva/eva/executor/grid_engine.py
RUN chmod 664 /opt/eva/eva/executor/grid_engine.py

USER eventadapter
ENV EVA_LOG_CONFIG /opt/eva/etc/logging.ini
CMD python3 -m eva
