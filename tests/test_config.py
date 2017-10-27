import os

import config_module
import pytest

import goblin
from goblin import driver, exception

dirname = os.path.dirname(os.path.dirname(__file__))


@pytest.fixture(params=[0, 1])
def conf_module(request):
    if request.param:
        return 'config_module'
    else:
        return config_module


def test_cluster_default_config(event_loop):
    cluster = driver.Cluster(event_loop)
    assert cluster.config['scheme'] == 'ws'
    assert cluster.config['hosts'] == ['localhost']
    assert cluster.config['port'] == 8182
    assert cluster.config['ssl_certfile'] == ''
    assert cluster.config['ssl_keyfile'] == ''
    assert cluster.config['ssl_password'] == ''
    assert cluster.config['username'] == ''
    assert cluster.config['password'] == ''


@pytest.mark.asyncio
async def test_app_default_config(event_loop):
    cluster = driver.Cluster(event_loop)
    app = goblin.Goblin(cluster)
    assert app.config['scheme'] == 'ws'
    assert app.config['hosts'] == ['localhost']
    assert app.config['port'] == 8182
    assert app.config['ssl_certfile'] == ''
    assert app.config['ssl_keyfile'] == ''
    assert app.config['ssl_password'] == ''
    assert app.config['username'] == ''
    assert app.config['password'] == ''
    assert issubclass(app.config['message_serializer'],
                      driver.GraphSONMessageSerializer)
    await app.close()


def test_cluster_custom_config(event_loop, cluster_class):
    cluster = cluster_class(
        event_loop, username='dave', password='mypass', hosts=['127.0.0.1'])
    assert cluster.config['scheme'] == 'ws'
    assert cluster.config['hosts'] == ['127.0.0.1']
    assert cluster.config['port'] == 8182
    assert cluster.config['ssl_certfile'] == ''
    assert cluster.config['ssl_keyfile'] == ''
    assert cluster.config['ssl_password'] == ''
    assert cluster.config['username'] == 'dave'
    assert cluster.config['password'] == 'mypass'
    assert issubclass(cluster.config['message_serializer'],
                      driver.GraphSONMessageSerializer)


def test_cluster_config_from_json(event_loop, cluster_class):
    cluster = cluster_class(event_loop)
    cluster.config_from_file(dirname + '/tests/config/config.json')
    assert cluster.config['scheme'] == 'wss'
    assert cluster.config['hosts'] == ['localhost']
    assert cluster.config['port'] == 8182
    assert cluster.config['ssl_certfile'] == ''
    assert cluster.config['ssl_keyfile'] == ''
    assert cluster.config['ssl_password'] == ''
    assert cluster.config['username'] == 'dave'
    assert cluster.config['password'] == 'mypass'

    assert issubclass(cluster.config['message_serializer'],
                      driver.GraphSONMessageSerializer)


def test_cluster_config_from_yaml(event_loop, cluster_class):
    cluster = cluster_class(event_loop)
    cluster.config_from_file(dirname + '/tests/config/config.yml')
    assert cluster.config['scheme'] == 'wss'
    assert cluster.config['hosts'] == ['localhost']
    assert cluster.config['port'] == 8183
    assert cluster.config['ssl_certfile'] == ''
    assert cluster.config['ssl_keyfile'] == ''
    assert cluster.config['ssl_password'] == ''
    assert cluster.config['username'] == ''
    assert cluster.config['password'] == ''
    assert issubclass(cluster.config['message_serializer'],
                      driver.GraphSONMessageSerializer)


def test_cluster_config_from_module(event_loop, cluster_class, conf_module):
    cluster = cluster_class(event_loop)
    cluster.config_from_module(conf_module)
    assert cluster.config['scheme'] == 'wss'
    assert cluster.config['hosts'] == ['localhost']
    assert cluster.config['port'] == 8183
    assert cluster.config['message_serializer'] is driver.GraphSONMessageSerializer


@pytest.mark.asyncio
async def test_app_config_from_json(app):
    app.config_from_file(dirname + '/tests/config/config.json')
    assert app.config['scheme'] == 'wss'
    assert app.config['hosts'] == ['localhost']
    assert app.config['port'] == 8182
    assert app.config['ssl_certfile'] == ''
    assert app.config['ssl_keyfile'] == ''
    assert app.config['ssl_password'] == ''
    assert app.config['username'] == 'dave'
    assert app.config['password'] == 'mypass'

    assert issubclass(app.config['message_serializer'],
                      driver.GraphSONMessageSerializer)
    await app.close()


@pytest.mark.asyncio
async def test_app_config_from_yaml(app):
    app.config_from_file(dirname + '/tests/config/config.yml')
    assert app.config['scheme'] == 'wss'
    assert app.config['hosts'] == ['localhost']
    assert app.config['port'] == 8183
    assert app.config['ssl_certfile'] == ''
    assert app.config['ssl_keyfile'] == ''
    assert app.config['ssl_password'] == ''
    assert app.config['username'] == ''
    assert app.config['password'] == ''
    assert issubclass(app.config['message_serializer'],
                      driver.GraphSONMessageSerializer)
    await app.close()


@pytest.mark.asyncio
async def test_app_config_from_module(app, conf_module):
    app.config_from_module(conf_module)
    assert app.config['scheme'] == 'wss'
    assert app.config['hosts'] == ['localhost']
    assert app.config['port'] == 8183
    assert app.config['message_serializer'] is driver.GraphSONMessageSerializer
    await app.close()
