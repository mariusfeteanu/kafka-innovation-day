import faust
import pytest


## App
app = faust.App('test-example')

test_topic = app.topic('test_topic')


@app.agent(test_topic)
async def greet(messages):
    async for message in messages:
        print(message)
        yield message



## Tests
@pytest.fixture()
def test_app(event_loop):
    """passing in event_loop helps avoid 'attached to a different loop' error"""
    app.finalize()
    app.conf.store = 'memory://'
    app.flow_control.resume()
    return app

@pytest.mark.asyncio()
async def test_greet(test_app):
    async with greet.test_context() as agent:
        await agent.put("Hello there")
        await agent.put("General Kenobi.")
            # mocked_bar.send.assert_called_with('hey')
