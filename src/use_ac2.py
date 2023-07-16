import asyncio
import aiohttp
from datetime import datetime, date, timedelta
from nt_models import CabinClass, PriceFilter
from nt_parser import results_to_excel, convert_ac_response_to_models2
from nt_filter import filter_prices, filter_airbounds, AirBoundFilter
from ac_searcher2 import Ac_Searcher2
from utils import date_range
from types import SimpleNamespace


BATCH_SIZE = 50
ASYNC_LIMIT = 2


async def run_query(origins: list, destinations: list, start_dt: str, end_dt: str, passengers: int):
    dates = date_range(start_dt, end_dt)
    airbound_filter = AirBoundFilter(
        max_stops=4,
        airline_include=[],
        airline_exclude=[],
    )
    price_filter = PriceFilter(
        preferred_classes=[CabinClass.J, CabinClass.F],
        min_cabin_class_pct=70,
        max_miles_per_person=120000,
        max_cash_per_person=300,
    )

    airbounds = []
    connector = aiohttp.TCPConnector(limit=ASYNC_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [SimpleNamespace(ori=ori, des=des, date=date) for ori in origins for des in destinations for date in dates if ori != des]
        sublists = (tasks[i:i+BATCH_SIZE] for i in range(0, len(tasks), BATCH_SIZE))
        for sublist in sublists:
            try:
                acs = Ac_Searcher2()
                airbounds_sublist = await asyncio.gather(*(search_ac(session, acs, airbound.ori, airbound.des, airbound.date, passengers) for airbound in sublist))
                airbounds.extend([airbound for airbounds_chunks in airbounds_sublist for airbound in airbounds_chunks])
            except Exception as error:
                print(error)
                continue

    airbounds = filter_airbounds(airbounds, airbound_filter)
    airbounds = filter_prices(airbounds, price_filter)

    results = [result for x in airbounds for result in x.to_flatted_list()]
    results.sort(key=lambda k: (k['miles'], k['cash'], k['departure_time'], k['duration_in_all']))

    result_file_name = f'AC_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_{"-".join(origins)}_to_{"-".join(destinations)}.xlsx'
    results_to_excel(results, out_file_name=result_file_name)


async def search_ac(session, acs, ori, des, date, number_of_passengers):
    try:
        response = await acs.search_for(session, ori, des, date, number_of_passengers)
        status = response.status if hasattr(response, "status") else "N/A"
        print(f'search for {ori} to {des} on {date} - {status}')
        if not response.ok:
            print(f"{response.real_url}\n{await response.text()}")
            await asyncio.sleep(5)
        airbound = await convert_ac_response_to_models2(response)
        return airbound
    except Exception as error:
        print(error)
        return list()


async def main():
    tomorrow =  (date.today() + timedelta(days=1)).isoformat()
    one_year = (date.today() + timedelta(days=365)).isoformat()

    # await run_query(start_dt="2024-05-04", end_dt="2024-05-10", passengers=2, origins=["YVR"], destinations=["LHR", "AMS", "ZRH", "GVA", "LYS", "MXP", "INN", "BSL", "STR", "FRA", "MUC"])
    # await asyncio.sleep(60*5)

    # await run_query(start_dt="2024-05-25", end_dt="2024-06-05", passengers=2, origins=["LHR", "AMS", "ZRH", "GVA", "LYS", "MXP", "INN", "BSL", "STR", "FRA", "MUC"], destinations=["YVR"])
    # await asyncio.sleep(60*5)

    # await run_query(start_dt="2024-05-05", end_dt="2024-05-15", passengers=2, origins=["YVR", "YYC"], destinations=["LHR", "AMS", "ZRH", "GVA", "LYS", "MXP"])
    # await asyncio.sleep(60*5)

    # await run_query(start_dt="2024-05-25", end_dt="2024-06-10", passengers=2, origins=["LHR", "AMS", "ZRH", "GVA", "LYS", "MXP"], destinations=["YVR", "YYC"])
    # await asyncio.sleep(60*5)

    # await run_query(start_dt="2024-05-05", end_dt="2024-05-15", passengers=2, origins=["YYZ", "YUL"], destinations=["LHR", "AMS", "ZRH", "GVA", "LYS", "MXP"])
    # await asyncio.sleep(60*5)

    # await run_query(start_dt="2024-05-25", end_dt="2024-06-10", passengers=2, origins=["LHR", "AMS", "ZRH", "GVA", "LYS", "MXP"], destinations=["YYZ", "YUL"])
    # await asyncio.sleep(60*5)

    # await run_query(start_dt=tomorrow, end_dt=one_year, passengers=1, origins=["YVR"], destinations=["HKG"])
    # await asyncio.sleep(60*5)

    # await run_query(start_dt=tomorrow, end_dt=one_year, passengers=1, origins=["HKG"], destinations=["YVR"])
    # await asyncio.sleep(60*5)

    # await run_query(start_dt="2023-10-01", end_dt=one_year, passengers=1, origins=["YVR"], destinations=["TYO"])
    # await asyncio.sleep(60*5)

    # await run_query(start_dt="2023-10-01", end_dt=one_year, passengers=1, origins=["TYO"], destinations=["YVR"])
    # await asyncio.sleep(60*5)

    # await run_query(start_dt="2023-10-01", end_dt=one_year, passengers=1, origins=["YVR"], destinations=["OSA"])
    # await asyncio.sleep(60*5)

    # await run_query(start_dt="2023-10-01", end_dt=one_year, passengers=1, origins=["OSA"], destinations=["YVR"])



if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())