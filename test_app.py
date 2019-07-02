
import pytest

from app import *

## Tests
@pytest.fixture()
def test_app(event_loop):
    """passing in event_loop helps avoid 'attached to a different loop' error"""
    app.finalize()
    app.conf.store = 'memory://'
    app.flow_control.resume()
    return app


@pytest.mark.asyncio()
async def test_pii(test_app):
    async with mask_pii.test_context() as agent:
        marius = RegisteredUser(account_id='kjduebvfds',
                                email='marius.something@gmail.cob',
                                name='Marius')
        await agent.put(marius)
7
        print(marius.to_representation())

        assert len(agent.results) == 1
        
        masked_marius = agent.results[0]

        assert masked_marius.account_id == 'kjduebvfds'
        assert masked_marius.email =='boc.liamg@gnihtemos.suiram'
        assert masked_marius.name == 'suiraM'
