ARG from

FROM $from

ADD . /opt/siderpy
RUN pip install -e /opt/siderpy && pip install -r /opt/siderpy/tests/requirements.txt && pip uninstall -y hiredis
