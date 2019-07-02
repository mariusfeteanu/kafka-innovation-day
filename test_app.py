import pytest
import asyncio

from app import *

## Tests
@pytest.fixture()
def test_app(event_loop):
    """passing in event_loop helps avoid 'attached to a different loop' error"""
    app.finalize()
    if not 'memory' in str(app.conf.store):
        app.conf.store = 'memory://'
    app.flow_control.resume()
    return app

@pytest.mark.asyncio()
async def test_calculate_enquiries_per_date(test_app):
    async with calculate_enquiries_per_date.test_context() as agent:
        eq1 = EnquiryInitiated(enquiry_id='1',
                               date='2019-01-01',
                               product='A',
                               account_id='B')
        eq2 = EnquiryInitiated(enquiry_id='1',
                               date='2019-01-02',
                               product='A',
                               account_id='B')
        eq3 = EnquiryInitiated(enquiry_id='1',
                               date='2019-01-02',
                               product='B',
                               account_id='B')
        eq4 = EnquiryInitiated(enquiry_id='1',
                               date='2019-01-03',
                               product='B',
                               account_id='B')
        eq5 = EnquiryInitiated(enquiry_id='1',
                               date='2019-01-03',
                               product='B',
                               account_id='B')
        await agent.put(eq1)
        await agent.put(eq2)
        await agent.put(eq3)
        await agent.put(eq4)
        await agent.put(eq5)

    assert enquiry_initiated_table[('A', '2019-01-01')] == 1
    assert enquiry_initiated_table[('A', '2019-01-01')] == 1
    assert enquiry_initiated_table[('B', '2019-01-02')] == 1
    assert enquiry_initiated_table[('A', '2019-01-01')] == 1
    assert enquiry_initiated_table[('B', '2019-01-03')] == 2

@pytest.mark.asyncio()
async def test_pii(test_app):
    async with mask_pii.test_context() as agent:
        marius = RegisteredUser(account_id='kjduebvfds',
                                email='marius.something@gmail.cob',
                                name='Marius',
                                user_topic='topic')
        await agent.put(marius)

        print(marius.to_representation())
        #print(get_enriched_user(marius))

        assert len(agent.results) == 1

        masked_marius = agent.results[0]

        assert masked_marius.account_id == 'kjduebvfds'
        assert masked_marius.email =='boc.liamg@gnihtemos.suiram'
        assert masked_marius.name == 'suiraM'

@pytest.mark.asyncio()
async def test_animal_mapping(test_app):
    async with add_fav_animal.test_context() as agent:
        marius = RegisteredUser(account_id='kjduebvfds',
                                email='marius.something@gmail.cob',
                                name='Marius',
                                user_topic='topic')

        print(get_enriched_user(marius))

        await agent.put(get_enriched_user(marius))

        enriched_marius = agent.results[0]

        assert enriched_marius.fav_animal == 'dog'


# @pytest.mark.asyncio()
# async def test_full_enquiry_completed(test_app):
#     async with full_enquiry_completed.test_context() as agent:
#         # put some initiated enquiries
#         # put some completed enquiries
#         ei1 = EnquiryInitiated(enquiry_id='kjduebvfds',
#                                 date='2018-01-01',
#                                 product='insurances',
#                                 account_id='kjduebvfds')
#         await agent.put(ei1)
#
#         assert len(agent.results) == 1
