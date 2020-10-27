import os
import time

import docker
import pytest


PKG_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "..")


class ContainerExecutor:
    def __init__(self):
        self.containers = []
        self.cli = docker.from_env()

    def create(self, image, **kwds):
        defaults = {"detach": True, "volumes": {PKG_DIR: {"bind": "/opt/siderpy", "mode": "rw"}}}
        defaults.update(kwds)
        kwds = defaults
        container = self.cli.containers.create(image, **kwds)
        self.containers.append(container)
        return container

    def run(self, image, **kwds):
        container = self.create(image, **kwds)
        container.start()
        return container

    def run_wait_up(self, image, **kwds):
        container = self.run(image, **kwds)
        container.reload()
        tend = time.monotonic() + 10
        while container.status != "running" and time.monotonic() < tend:
            time.sleep(0.1)
        time.sleep(0.1)
        container.reload()
        if container.status != "running":
            print(container.logs().decode())
            raise Exception("Container error")
        return container

    def run_wait_exit(self, image, **kwds):
        container = self.run(image, **kwds)
        container.reload()
        container.wait()
        return container


@pytest.fixture(scope="session")
def container_executor():
    container_executor = ContainerExecutor()
    try:
        yield container_executor
    finally:
        for container in container_executor.containers:
            container.remove(force=True)


@pytest.fixture(scope="function")
def redis(container_executor):
    yield container_executor.run_wait_up("redis:6", command='redis-server --save "" --appendonly no')


class Test:

    images = [
        pytest.param("siderpy_tests_3.7", id="py3.7"),
        pytest.param("siderpy_tests_3.8", id="py3.8"),
        pytest.param("siderpy_tests_hiredis_3.7", id="hiredis_py3.7"),
        pytest.param("siderpy_tests_hiredis_3.8", id="hiredis_py3.8"),
    ]

    envs = [
        pytest.param([], id="no_ssl"),
        pytest.param(["TESTS_USE_SSL=1"], id="ssl"),
    ]

    commands = [
        pytest.param("pytest --timeout=15 -sv /opt/siderpy/tests/tests_small.py --cov", id="small"),
        pytest.param("pytest --timeout=30 -sv /opt/siderpy/tests/tests_medium.py", id="medium"),
    ]

    @pytest.mark.parametrize("image", images)
    @pytest.mark.parametrize("env", envs)
    @pytest.mark.parametrize("command", commands)
    def test_main(self, redis, container_executor, image, env, command):
        environment = ["REDIS_HOST=redis", "REDIS_PORT=6379"]
        links = {redis.id: "redis"}
        if "TESTS_USE_SSL=1" in env:
            haproxy = container_executor.run_wait_up(
                "haproxy:latest",
                command="haproxy -f /opt/siderpy/tests/haproxy.cfg",
                links={redis.id: "redis"},
                environment=["REDIS_HOST=redis", "REDIS_PORT=6379"],
            )
            environment = ["REDIS_HOST=haproxy", "REDIS_PORT=6379"]
            links.update({haproxy.id: "haproxy"})
        container = container_executor.run_wait_exit(image, command=command, environment=environment + env, links=links)
        print(container.logs().decode())
        data = container.wait()
        assert data["StatusCode"] == 0


class TestBenchmark:

    envs = [pytest.param([], id="asyncio"), pytest.param(["UVLOOP=1"], id="uvloop")]

    @pytest.mark.parametrize("env", envs)
    def test_benchmark(self, redis, container_executor, env):
        image = "siderpy_tests_hiredis_3.8"
        command = "pytest /opt/siderpy/tests/tests_benchmark.py"
        environment = ["REDIS_HOST=redis", "REDIS_PORT=6379"] + env
        links = {redis.id: "redis"}
        container = container_executor.run_wait_exit(image, command=command, environment=environment, links=links)
        print(container.logs().decode())
        data = container.wait()
        assert data["StatusCode"] == 0
