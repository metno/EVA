# Dockerfile for EVA.

FROM metno/eva-base
RUN git clone --single-branch -b adam --depth 1 https://github.com/metno/eva.git /opt/eva
RUN pip3 install --upgrade pip setuptools flake8
RUN pip3 install --ignore-installed six requests
RUN pip3 install -e /opt/eva/
RUN cd /opt/eva && make lint && make test
USER eventadapter
ENV EVA_LOG_CONFIG /opt/eva/etc/logging.ini
CMD python3 -m eva
