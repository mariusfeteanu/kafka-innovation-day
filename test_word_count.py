import faust
import pytest


## App
app = faust.App('test-example')

test_topic = app.topic('test_topic')
word_count_table = app.Table('word-count', default=int)


@app.agent(test_topic)
async def greet(messages):
    async for message in messages:
        for word in message.split():
            word = word.lower().strip('!.?')
            if word:
                word_count_table[word] += 1
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
        await agent.put("over there.")

    assert word_count_table['hello'] == 1
    assert word_count_table['there'] == 2
    assert word_count_table['general'] == 1
    assert word_count_table['kenobi'] == 1
    assert word_count_table['over'] == 1
            # mocked_bar.send.assert_called_with('hey')
