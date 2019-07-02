import faust

app = faust.App('test-example')

class Order(faust.Record, serializer='json'):
    account_id: str
    product_id: str
    amount: int
    price: float

orders_topic = app.topic('orders', value_type=Order)
orders_for_account = app.Table('order-count-by-account', default=int)

@app.agent(orders_topic)
async def order(orders):
    async for order in orders.group_by(Order.account_id):
        orders_for_account[order.account_id] += 1
        yield order

async def test_order():
    # start and stop the agent in this block
    async with order.test_context() as agent:
        o = Order(account_id='1', product_id='2', amount=1, price=300)
        # sent order to the test agents local channel, and wait
        # the agent to process it.
        await agent.put(o)
        # at this point the agent already updated the table
        assert orders_for_account[o.account_id] == 1
        await agent.put(o)
        assert orders_for_account[o.account_id] == 2

async def run_tests():
    app.conf.store = 'memory://'   # tables must be in-memory
    await test_order()

if __name__ == '__main__':
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_tests())
