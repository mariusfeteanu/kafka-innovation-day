import faust

app = faust.App('test-example')

test_topic = app.topic('test_topic')


@app.agent(test_topic)
async def greet(messages):
    async for message in messages:
        print(message)
        yield message


async def test_greet():
    # start and stop the agent in this block
    async with greet.test_context() as agent:
        await agent.put("Hello there")
        await agent.put("General Kenobi.")


async def run_tests():
    app.conf.store = 'memory://'   # tables must be in-memory
    await test_greet()


if __name__ == '__main__':
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_tests())
