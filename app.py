import faust


## App
app = faust.App('test-example')

class RegisteredUser(faust.Record, serializer='json'):
    account_id: str
    email: str
    name: str
    user_topic: str

class EnrichedUser(faust.Record, serializer='json'):
    account_id: str
    email: str
    name: str
    user_topic: str
    fav_animal:str



class EnquiryInitiated(faust.Record, serializer='json'):
    enquiry_id: str
    date: str
    product: str
    account_id: str

class EnquiryCompleted(faust.Record, serializer='json'):
    enquiry_id: str
    date: str
    best_partner: str

class SaleCompleted(faust.Record, serializer='json'):
    sale_id: str
    enquiry_id: str
    date: str
    amount: float

class FullEnquiryCompleted(faust.Record, serializer='json'):
    enquiry_id: str
    completed_date: str
    product: str
    best_partner: str

registered_topic = app.topic('registered_topic')
enriched_topic = app.topic('enriched_topic')
enquiry_initiated_topic = app.topic('enquiry_initiated_topic')
enquiry_completed_topic = app.topic('enquiry_completed_topic')
sale_completed_topic = app.topic('sale_completed_topic')

enquiry_initiated_table = app.Table('(product, date)', default=int)


def mask_user(user):
    return RegisteredUser(
        account_id=user.account_id,
        email=user.email[::-1],
        name=user.name[::-1],
        user_topic=user.user_topic[::-1]
    )


def get_enriched_user(user):
    return EnrichedUser(
        account_id=user.account_id,
        email=user.email,
        name=user.name,
        user_topic=user.user_topic,
        fav_animal=get_pet_by_user_id(user.account_id)
    )


preferences = {
               '1': 'cat',
               '2': 'cat',
               '3': 'dog',
               '4': 'cat',
               '5': 'dog',
               '6': 'dog',
               '7': 'cat',
               'kjduebvfds': 'dog'
}


def get_pet_by_user_id(user_id):
    return preferences[user_id]


@app.agent(registered_topic)
async def mask_pii(registered_users):
    async for registered_user in registered_users:
        masked_user = mask_user(registered_user)
        yield masked_user

@app.agent(enriched_topic)
async def add_fav_animal(registered_users):
    async for registered_user in registered_users:
        enriched_user = get_enriched_user(registered_user)
        yield enriched_user

@app.agent(enquiry_initiated_topic)
async def calculate_enquiries_per_date(enquiries_initiated):
    async for enquiry_initiated in enquiries_initiated:
        enquiry_initiated_table[(enquiry_initiated.product, enquiry_initiated.date)] += 1
        yield enquiry_initiated


# @app.task()
# async def full_enquiry_completed_stream():
#     enquiry_initiated = enquiry_initiated_topic.stream()
#     enquiry_completed = enquiry_completed_topic.stream()
#     print("STARTED: full_enquiry_completed_stream")
#     async for event in (enquiry_initiated & enquiry_completed).join():
#         print("EVENT: ", event)
#         yield event
#     print("STOP: Falling out of task")

#
# @app.agent()
# async def somethingsomething(full_enquiry_completed_stream):
#     async for event in full_enquiry_completed_stream:
#         yield event
