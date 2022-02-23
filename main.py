import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
from time import time

REQUEST_PER_ENDPOINT = 50


async def api_get_request(session, url):
    async with session.get(url) as response:
        assert response.status == 200
        body = await response.text()
        return json.loads(body)


async def get_endpoint_urls(session, url):
    endpoint_urls = await api_get_request(session, url)
    result = list(map(lambda el: 'http:' + str(el), endpoint_urls['common']))
    return result


async def clean_content_from_subevent(content: dict) -> dict:
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

    filtered_events = list(filter(lambda event: event['name'] not in subevents, content['events']))

    cleaned_content = dict(content)
    cleaned_content['events'] = filtered_events

    return cleaned_content


async def rich_events_by_section(content: dict) -> dict:
    new_content = dict(content)
    for section in content['sections']:
        for event_id in section['events']:
            new_content['events'][int(event_id) - 1]['section_name'] = section['name']

    return new_content


def save_json_data(content, filename):
    with open(filename, "w") as file:
        file.write(json.dumps(content))


async def parse_pipeline(session, url: str):
    print(f'start parsing: {url}')
    content = await api_get_request(session, url)
    content = await rich_events_by_section(content)
    content = await clean_content_from_subevent(content)
    save_json_data(content, f'{url[-10:]}')
    print(f'end parsing: {url}')


def query_gen(start_period: datetime, end_period: datetime):
    cursor = start_period
    while cursor <= end_period:
        yield f'/results/results.json.php?lineDate={cursor.strftime("%Y-%m-%d")}'
        cursor += timedelta(days=1)


async def parse_period(start_period: datetime, end_period: datetime):
    async with aiohttp.ClientSession() as session:
        available_endpoints = await get_endpoint_urls(session, 'https://www.fonbet.ru/urls.json')

    issue_list = [q for q in query_gen(start_period, end_period)]

    async with aiohttp.ClientSession() as session:
        while len(issue_list) > 0:
            task_list = []
            for endpoint in available_endpoints:
                for i in range(0, REQUEST_PER_ENDPOINT):
                    if len(issue_list) > 0:
                        url = endpoint + issue_list.pop(0)
                        task = asyncio.create_task(parse_pipeline(session, url))
                        task_list.append(task)
            await asyncio.gather(*task_list)


if __name__ == "__main__":
    t0 = time()
    start = datetime.now() - timedelta(days=365)
    end = datetime.now()

    asyncio.run(parse_period(start, end))
    print(f'{time() - t0}')
