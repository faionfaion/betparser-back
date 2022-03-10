import asyncio
import json
import datetime
import re
from aiohttp import web
import aiohttp_cors
import motor.motor_asyncio
from time import time
from fonbet_parser import parse_period

routes = web.RouteTableDef()


@routes.get('/api/v1/runparser')
async def events_get_endpoint(request: web.Request):
    t0 = time()
    start = datetime.datetime.now() - datetime.timedelta(days=365)
    end = datetime.datetime.now()

    await parse_period(start, end)
    return web.Response(text=f'Parse time: {str(t0 - time())}')


@routes.get('/api/v1/events')
async def events_get_endpoint(request: web.Request):
    if 'search' in list(request.query.keys()):
        body = await search_by_title(request.query['search'])
    elif 'date' in list(request.query.keys()):
        body = await search_by_date(request.query['date'])
    else:
        body = await search_by_date()

    headers = {'content-type': 'application/json'}
    return web.Response(text=json.dumps(body), status=200, headers=headers)


async def search_by_date(date_string: str = None):
    if date_string is None:
        await parse_period(datetime.datetime.now(), datetime.datetime.now())
        date_string = str(datetime.datetime.utcnow().date())
    try:
        format = "%Y-%m-%d %Z"
        date = datetime.datetime.strptime(date_string + " UTC", format)
    except ValueError:
        return {'error': 'This is the incorrect date string format. It should be YYYY-MM-DD'}

    db = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/')
    collection = db['fonbetdata']['event']

    start_date = int(date.timestamp())
    end_date = start_date + 86400

    cursor = collection.find({'$and': [{'startTime': {'$gte': start_date}}, {'startTime': {'$lt': end_date}}]},
                             {'_id': 0})

    return await cursor.to_list(length=10000)


async def search_by_title(search_query: str):
    db = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/')
    collection = db['fonbetdata']['event']

    cursor = collection.find({'name': {'$regex': '^' + re.escape(search_query)}}, {'_id': 0})
    return await cursor.to_list(length=100)


app = web.Application()
app.add_routes(routes)

cors = aiohttp_cors.setup(app, defaults={
    "*": aiohttp_cors.ResourceOptions(
        allow_credentials=True,
        expose_headers="*",
        allow_headers="*",
    )
})

for route in app.router.routes():
    cors.add(route)

web.run_app(app)