import asyncio
import random
import logging
from contextlib import suppress

import pytest

from ubii.framework.protocol import AbstractProtocol, RunProtocol
#from ubii.framework.default_protocol import DefaultProtocol, States as UbiiStates
from ubii.node.node_protocol import DefaultProtocol, States as UbiiStates

pytestmark = pytest.mark.asyncio
log = logging.getLogger(__name__)


class MockProtocol(AbstractProtocol[UbiiStates]):
    starting_state = UbiiStates.STARTING
    end_state = UbiiStates.STOPPED

    async def _on_start(self, context):
        log.info(context)
        context.data = "test"
        await self.state.set(UbiiStates.CONNECTED)

    async def _on_connect(self, context):
        log.info(context)
        assert context.data == 'test'

        async def stop_at_some_point():
            await asyncio.sleep(random.randrange(5, 10))
            await self.state.set(UbiiStates.STOPPED)

        context.stop_task = asyncio.create_task(stop_at_some_point())

    async def _on_stopped(self, context):
        log.info(context)
        context.stop_task.cancel()
        await context.stop_task

    state_changes = {
        (None, UbiiStates.STARTING): _on_start,
        (UbiiStates.STARTING, UbiiStates.CONNECTED): _on_connect,
        (UbiiStates.CONNECTED, UbiiStates.STOPPED): _on_stopped,
    }


async def test_mock_protocol():
    protocol = MockProtocol()
    coro = RunProtocol(protocol)
    await asyncio.create_task(coro)


@pytest.fixture
async def protocol(request):
    protocol: AbstractProtocol = request.param()
    run: asyncio.Task = protocol.start()
    yield protocol

    run.cancel()
    with suppress(asyncio.CancelledError):
        await protocol

    assert run.done()
    assert protocol.trigger_sentinel.is_set()

    # if the sentinel callbacks schedule callbacks themselves (e.g. the aiohttp websocket connection is scheduled to
    # close when the context is closed, which happens in the default protocol when the sentinel callbacks are run)
    # these callbacks might not be run immediately. pytest-asyncio might close the event loop if we don't sleep here.
    # there is nothing we can do about that, but it's in line with the aiohttp documentation telling you to sleep
    # before closing the loop.
    await asyncio.sleep(1)


@pytest.mark.parametrize('protocol', [DefaultProtocol], indirect=True)
async def test_default_protocol(protocol: DefaultProtocol):
    await asyncio.wait_for(protocol.state.get(predicate=lambda state: state == UbiiStates.CONNECTED),
                           timeout=5)
    client = protocol.client
    assert client.id


@pytest.mark.parametrize('protocol', [DefaultProtocol], indirect=True)
async def test_default_protocol_stop(protocol: DefaultProtocol):
    await asyncio.wait_for(protocol.state.get(predicate=lambda state: state == UbiiStates.CONNECTED),
                           timeout=5)
    client = protocol.client
    assert client.id
    await protocol.stop()

    assert protocol.state.value == protocol.end_state
