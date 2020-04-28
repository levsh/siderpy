ARG from
ARG extensions=

FROM $from

ADD . /opt/siderpy
RUN pip install -e /opt/siderpy$extensions && pip install pytest-asyncio pytest-timeout pytest-cov
