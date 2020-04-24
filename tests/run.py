import logging
import os
import time

import docker
import pytest


PKG_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')


class ContainerExecutor:

    def __init__(self):
        self.containers = []
        self.cli = docker.from_env()

    def create(self, image, **kwds):
        defaults = {
                'detach': True,
                'volumes': {PKG_DIR: {'bind': '/opt/siderpy', 'mode': 'rw'}}
        }
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
        while container.status != 'running':
            time.sleep(0.1)
        return container

    def run_wait_exit(self, image, **kwds):
        container = self.run(image, **kwds)
        container.reload()
        container.wait()
        return container


@pytest.fixture(scope='session')
def container_executor():
    container_executor = ContainerExecutor()
    try:
        yield container_executor
    finally:
        for container in container_executor.containers:
            container.remove(force=True)


@pytest.fixture(scope='function')
def redis(container_executor):
    yield container_executor.run_wait_up('redis:latest')


class Test:

    @pytest.mark.parametrize('test_image', ['siderpy_tests_3.7', 'siderpy_tests_3.8'])
    def test(self, redis, container_executor, test_image):
        container = container_executor.run_wait_exit(
                test_image,
                command='pytest --timeout=15 -s -v --durations=0 /opt/siderpy/tests/tests.py',
                links={redis.id: 'redis'})
        print(container.logs().decode())
        data = container.wait()
        assert data['StatusCode'] == 0
