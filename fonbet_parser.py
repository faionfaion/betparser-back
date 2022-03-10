import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
from time import time
from aiohttp.client_exceptions import ClientError, ServerDisconnectedError
import logging
import sys
import motor.motor_asyncio

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(asctime)s:[%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger('FonbetParser')


class FonbetParserException(Exception):
    pass


class Endpoint:
    def __init__(self, base_url: str):
        self.__base_url = base_url

    async def __call__(self, session: aiohttp.ClientSession, query: str) -> (bool, dict):
        try:
            logger.debug(f'add task for parsing {self.__base_url + query}')
            data = await self.__api_get_request(session, query)
            logger.debug(f'DONE {self.__base_url + query}.')
            return True, data

        except (ClientError, ServerDisconnectedError) as e:
            logger.warning(e)
            return False, {'error': str(e)}

        except json.decoder.JSONDecodeError as e:
            logger.warning(e)
            return False, {'error': str(e)}

    async def __api_get_request(self, session, query) -> dict:
        async with session.get(self.__base_url + query) as response:
            body = await response.text()
            return json.loads(body)


class FonbetEndpointManager:
    def __init__(self):
        loop = asyncio.get_event_loop()
        task = loop.create_task(self.__ainit())
        asyncio.gather(task)

    def __aiter__(self):
        return self

    async def __anext__(self) -> Endpoint:
        while not hasattr(self, '_FonbetEndpointManager__endpoint_list'):
            await asyncio.sleep(1 / 100)
        while True:
            endpoint = self.__endpoint_list.pop(0)
            self.__endpoint_list.append(endpoint)
            return endpoint

    async def __call__(self) -> Endpoint:
        return await self.__anext__()

    async def __ainit(self):
        async with aiohttp.ClientSession() as session:
            async with session.get('https://www.fonbet.ru/urls.json') as response:
                assert response.status == 200
                body = await response.text()
                endpoint_urls = json.loads(body)
        endpoint_urls = list(map(lambda el: 'http:' + str(el), endpoint_urls['common']))
        self.__endpoint_list = [Endpoint(url) for url in endpoint_urls]


def list_roller(array: list):
    assert len(array) > 1, f'array mus have more then 2 elements for rolling.\n List: {array}'
    array = list(array)
    while True:
        next_element = array.pop(0)
        array.append(next_element)
        yield next_element


def prepare_fonbet_data_to_save(content: dict) -> dict:
    subevents = ["1-й", "1st", "2-й", "2nd", "3-й", "3rd", "4-й", "4th", "5-й", "5th", "6-й", "6th", "7-й", "7th",
                 "8-й", "8th", "9-й", "9th", "10-й", "10th", "11-й", "11th", "1-j", "2-j", "3-j", "1-я карта",
                 "2-я карта", "3-я карта", "4-я карта", "5-я карта", "доп.выбор", "alternate", "исход 75 минут",
                 "75min", "исход 60 минут", "60min", "исход 30 минут", "30min", "исход 15 минут", "15min",
                 "Основное время", "Reguar time", "Regular time", "Таймы", "Половины", "Halves", "основное время",
                 "48 min", "40 min", "9 innings", "овертайм", "overtime", "доп. время", "дополнительное время",
                 "extra time", "очки", "points", "60 min", "с учётом ОТ", "incl OT", "5 innings", "угловые", "corners",
                 "жёлтые карты", "yellow cards", "фолы", "fouls", "удары в створ", "shots on goal", "офсайды",
                 "offsides", "удары всего", "total shots", "штанги или перекладины", "to hit the woodwork", "замены",
                 "substitutions", "вброс аутов", "throw-ins", "удары от ворот", "goal kicks", "заработает фолов",
                 "fouls suffered", "голы", "goals", "очки (гол+пас)", "points (goals+assists)", "броски в створ",
                 "голы в большинстве", "power play goals", "вбрасывания", "face offs", "большинство", "power plays",
                 "штрафное время", "penalty min", "мин в большинстве", "PP min", "Баскетбол", "Basketball",
                 "заб/штрафные", "free throws", "заб/2-х очковые", "2-points made", "заб/3-х очковые", "3-points made",
                 "подборы", "rebounds", "передачи", "assists", "перехваты", "steals", "потери", "turnovers", "блокшоты",
                 "blocked shots", "%", "эйсы", "aces", "двойные", "double faults", "невынужденные ошибки",
                 "unforced errors", "брейкпойнты", "breakpoints", "брейки", "breakpoints won",
                 "наивысшая скорость подачи км/ч", "fastest serve km/h", "промахи", "shots missed",
                 "дополнительные патроны", "extra bullets", "штрафные круги", "penalty loops", "shots",
                 "заб/2-х очковые", "2-point shots", "заб/3-х очковые", "3-point shots", "доп. патроны", "штр. круги",
                 "исход 10 минут", "10min", "исход 5 минут", "5min", "серия буллитов", "penalty shootouts",
                 "серия пенальти", "penalty shootouts", "золотой сет", "golden set", "Овертаймы", "Overtimes",
                 "силовые приёмы", "hits", "филд голы", "field goals", "тачдауны", "touchdowns",
                 "кол-во 2-мин удалений", "number of 2min penalty", "тотал 1-х даунов", "total first downs",
                 "ошибки на подаче", "service errors", "Забьёт гол и счёт матча", "Anytime goalscorer scorecast",
                 "Забьёт гол и исход матча", "Anytime goalscorer wincast", "блоки", "blocks", "видеопросмотры",
                 "video reviews", "касания мяча вратарём", "goalkeeper touches", "пробег (км)", "distance covered (km)",
                 "попытки", "tries", "голы в больш", "power play goals", "буллиты", "shootouts", "7-метровые броски",
                 "7m shots", "заб/броски (1 очко)", "1-point made", "заб/броски (2 очка)", "выход мед. бригады на поле",
                 "medical staff to enter the field", "video referee", "Киллы", "Kills", "Экстра энд", "Extra end"]
    content = dict(content)

    for section in content['sections']:
        for event_id in section['events']:
            content['events'][int(event_id) - 1]['section_name'] = section['name']

    filtered_events = list(filter(lambda event: not any([event['name'].find(tag) != -1 for tag in subevents]), content['events']))
    logger.debug(f"Prepared events to saving for date {datetime.utcfromtimestamp(int(content['events'][0]['startTime'])).strftime('%Y-%m-%d')}:"
                 f" {len(content['events'])} events filtered to {len(filtered_events)}")
    content['events'] = filtered_events
    return content


def fonbet_period_query_gen(start_period: datetime = None, end_period: datetime = None) -> list:
    if start_period is None:
        start_period = datetime.now()
    if end_period is None:
        end_period = datetime.now()

    assert start_period <= end_period, f'start_period must be less or equal then end_period \n' \
                                       f'start_period: {start_period}\n' \
                                       f'end_period: {end_period}'

    cursor = start_period
    result = []
    while cursor <= end_period:
        result.append(f'/results/results.json.php?lineDate={cursor.strftime("%Y-%m-%d")}')
        cursor += timedelta(days=1)
    return result


async def save_fonbet_data(mongo_collection: motor.motor_asyncio.AsyncIOMotorCollection, content):
    await mongo_collection.insert_many(content)
    logger.debug(f'Saved to db {len(content["events"])} elements.')


async def parse_pipeline(session: aiohttp.ClientSession,
                         endpoint_manager: FonbetEndpointManager,
                         query: str,
                         mongo_collection: motor.motor_asyncio.AsyncIOMotorCollection):

    endpoint = await endpoint_manager()
    status = False
    recursion_iterator = 0
    content = {}
    while not status:
        try:
            status, content = await endpoint(session, query)
        except UnicodeDecodeError:
            logger.error(f'Received broken data for query: {query}')
            return
        except asyncio.exceptions.TimeoutError:
            logger.info(f'Timeout for query {query}. Try to change endpoint.')
        if not status:
            await asyncio.sleep(recursion_iterator)
            recursion_iterator += 1
            if recursion_iterator >= 10:
                logger.error(f'ERROR parse {query}')
                return
    content = prepare_fonbet_data_to_save(content)
    await save_fonbet_data(mongo_collection, content)


async def parse_period(start_period: datetime, end_period: datetime):
    assert start_period <= end_period, f'start_period must be less then end_period \n' \
                                       f'start_period: {start_period}\n' \
                                       f'end_period: {end_period}'

    db_client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/')
    mongo_collection = db_client['fonbetdata']['event']


    link_list = fonbet_period_query_gen(start_period, end_period)
    urls_to_parse = [issue for issue in link_list]
    endpoint_manager = FonbetEndpointManager()

    await mongo_collection.remove({'$and': [{'startTime': {'$gte': start_period.timestamp()}},
                                            {'startTime': {'$lt': end_period.timestamp()}}]}, {'_id': 0})

    async with await db_client.start_session():
        connector = aiohttp.TCPConnector(limit_per_host=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            task_list = [asyncio.create_task(parse_pipeline(session, endpoint_manager, issue, mongo_collection)) for issue in urls_to_parse]
            await asyncio.gather(*task_list)

    await mongo_collection.create_index([('name', 1), ('startTime', -1)], {'unique': True})





