import faust


## App
app = faust.App('test-example')

class RegisteredUser(faust.Record, serializer='json'):
    account_id: str
    email: str
    name: str

class EnquiryInitiated(faust.Record, serializer='json'):
    enquiry_id: str
    date: str
    product: str

class EnquiryCompleted(faust.Record, serializer='json'):
    enquiry_id: str
    date: str

class SaleCompleted(faust.Record, serializer='json'):
    sale_id: str
    enquiry_id: str
    date: str
    amount: float

registered_topic = app.topic('registered_topic')
enquiry_initiated_topic = app.topic('enquiry_initiated_topic')
enquiry_completed_topic = app.topic('enquiry_completed_topic')
sale_completed_topic = app.topic('sale_completed_topic')


def mask_user(user):
    return RegisteredUser(
        account_id=user.account_id,
        email=user.email[::-1],
        name=user.name[::-1])


@app.agent(registered_topic)
async def mask_pii(registered_users):
    async for registered_user in registered_users:
        masked_user = mask_user(registered_user)
        yield masked_user
