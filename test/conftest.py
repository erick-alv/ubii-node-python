from __future__ import annotations

import asyncio
import logging
import pytest
import typing as t
import yaml

import ubii.proto as ub
from ubii.framework.client import Devices, UbiiClient, Services
#from ubii.framework.default_protocol import DefaultProtocol
from ubii.node.node_protocol import DefaultProtocol
from ubii.framework.logging import logging_setup

__verbosity__: int | None = None
ALWAYS_VERBOSE = True


def pytest_addoption(parser):
    parser.addoption(
        "--log-config", action="store", default="./data/logging_config.yaml",
        help="path to yaml file containing log config"
    )


def pytest_configure(config):
    global __verbosity__
    global ALWAYS_VERBOSE
    __verbosity__ = (
        logging.INFO - 10 * config.getoption('verbose')
        if not ALWAYS_VERBOSE else
        logging.DEBUG
    )

    import ubii.proto
    assert ubii.proto.__proto_package__ is not None, "No proto package set, aborting test setup."


@pytest.fixture(autouse=True, scope='session')
def configure_logging(request):
    log_config = logging_setup.change(verbosity=__verbosity__)

    from pathlib import Path
    log_config_path = Path(request.config.getoption('--log-config'))
    if log_config_path.exists():
        with log_config_path.open() as f:
            test_logging_config = yaml.safe_load(f)
            log_config.change(config=test_logging_config)

    custom = getattr(request, 'param', None)

    with logging_setup.change(config=custom, verbosity=__verbosity__):
        yield


@pytest.fixture(scope='session', autouse=True)
def service_url_env():
    import os
    from ubii.framework.constants import UBII_URL_ENV
    old = os.environ.get(UBII_URL_ENV)
    os.environ[UBII_URL_ENV] = 'http://localhost:8102/services/json'
    yield
    if old:
        os.environ[UBII_URL_ENV] = old


@pytest.fixture(autouse=True, scope='session')
def enable_debug():
    from ubii.framework.logging import debug
    previous = debug()
    debug(enabled=True)
    yield
    debug(enabled=previous)


@pytest.fixture(scope='session', autouse=True)
def event_loop():
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope='class')
async def base_client() -> UbiiClient:
    """
    We need more control over the client, so don't use the default client interface
    """
    protocol = DefaultProtocol()
    client = UbiiClient(protocol=protocol)

    protocol.client = client

    yield client
    client = await client
    await client.protocol.stop()


@pytest.fixture(scope='session')
def base_module():
    yield ub.ProcessingModule()


@pytest.fixture(scope='session')
def base_session():
    yield ub.Session()


@pytest.fixture
def register_device(client):
    async def _register(*args, **kwargs):
        _client = await client
        await _client.implements(Devices)
        _client[Devices].register_device(*args, **kwargs)

    yield _register


@pytest.fixture(scope='class')
async def start_session(client_spec):
    _started = []

    async def _start(session):
        client = await client_spec
        if session.id:
            raise ValueError(f"Session {session} already started.")

        nonlocal _started
        assert client.implements(Services)
        response = await client[Services].services.session_runtime_start(session=session)
        await asyncio.sleep(3)  # session needs to start up
        _started.append(response.session)

    yield _start

    for session in _started:
        pass
        # raises error
        # await client_spec.services.session_runtime_stop(session=session)


#P = t.TypeVar('P', bound=ub.ProtoMessage)
from proto import Message as ProtoMessage
P = t.TypeVar('P', bound=ProtoMessage)


def _change_specs(proto: P, *specs: P):
    base = type(proto).pb(proto)
    for change in specs:
        base.MergeFrom(type(change).pb(change))

    type(proto).copy_from(proto, base)


get_param = (lambda request: request.param if hasattr(request, 'param') else ())


@pytest.fixture(scope='class')
def module_spec(base_module, request):
    _change_specs(base_module, *get_param(request))
    yield base_module


@pytest.fixture(scope='class')
def client_spec(base_client, module_spec, request):
    _change_specs(base_client, *get_param(request))
    if module_spec not in base_client.processing_modules:
        base_client.processing_modules += [module_spec]

    yield base_client


@pytest.fixture(scope='class')
def session_spec(base_session, module_spec, request):
    _change_specs(base_session, *get_param(request))
    if module_spec not in base_session.processing_modules:
        base_session.processing_modules += [module_spec]
    yield base_session


# convenience
@pytest.fixture(scope='class')
def client(client_spec):
    return client_spec


def pytest_generate_tests(metafunc):
    specs = [
        'client_spec',
        'module_spec',
        'session_spec'
    ]

    for spec in specs:
        if hasattr(metafunc.cls, spec) and spec in metafunc.fixturenames:
            metafunc.parametrize(spec, getattr(metafunc.cls, spec), indirect=[spec])
