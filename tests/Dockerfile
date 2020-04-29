ARG from

FROM $from

ADD . /opt/siderpy
RUN pip install -e /opt/siderpy && pip install pytest-asyncio pytest-timeout pytest-cov
