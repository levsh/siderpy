ARG from

FROM $from

ADD . /opt/siderpy
WORKDIR /opt/siderpy

RUN pip install -e /opt/siderpy && pip install -r tests/requirements.txt && pip uninstall -y hiredis
