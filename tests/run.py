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

    images = [
            'siderpy_tests_3.7',
            'siderpy_tests_3.8',
            'siderpy_tests_hiredis_3.7',
            'siderpy_tests_hiredis_3.8',
    ]

    envs = [
            [],
            ['TESTS_USE_SSL=1'],
    ]

    commands = [
            'pytest --timeout=15 -sv /opt/siderpy/tests/tests_small.py --cov',
            'pytest --timeout=30 -sv --durations=5 /opt/siderpy/tests/tests.py',
    ]

    @pytest.mark.parametrize('image', images)
    @pytest.mark.parametrize('env', envs)
    @pytest.mark.parametrize('command', commands)
    def test_main(self, redis, container_executor, image, env, command):
        env_redis = ['REDIS_HOST=redis', 'REDIS_PORT=6379']
        links = {redis.id: 'redis'}
        if 'TESTS_USE_SSL=1' in env:
            haproxy = container_executor.run_wait_up(
                    'haproxy:latest',
                    command='haproxy -f /opt/siderpy/tests/haproxy.cfg',
                    links={redis.id: 'redis'},
                    environment=['REDIS_HOST=redis', 'REDIS_PORT=6379'])
            env_redis = ['REDIS_HOST=haproxy', 'REDIS_PORT=6379']
            links.update({haproxy.id: 'haproxy'})
        container = container_executor.run_wait_exit(
                image,
                command=command,
                environment=env_redis + env,
                links=links)
        print(container.logs().decode())
        data = container.wait()
        assert data['StatusCode'] == 0
