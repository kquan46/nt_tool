import asyncio
import aiohttp
from datetime import datetime
from nt_models import CabinClass, PriceFilter
from nt_parser import results_to_excel, convert_ac_response_to_models2
from nt_filter import filter_prices, filter_airbounds, AirBoundFilter
from ac_searcher2 import Ac_Searcher2
from utils import date_range
from types import SimpleNamespace

BATCH_SIZE = 100


async def main():
    origins = [
        # 'YVR',
        'TYO',
        # 'YYZ',
        # 'YYC',
        # 'YUL',
<<<<<<< HEAD
        # 'LHR', 'AMS', 'BRU', "ZRH", "GVA", "NCE", "CDG", "LYS", "MXP",
        # 'IST',
        ]

    start_dt = '2024-05-07'
    end_dt = '2024-05-25'
=======
        # 'LHR',
        # 'AMS',
        # 'BRU',
        # "ZRH",
        # "GVA",
        # "NCE",
        # "CDG",
        # "LYS",
        # "MXP",
        # 'IST',
        ]
    destinations = [
        'YVR',
        # 'TYO',
        # 'YYZ',
        # 'YYC',
        # 'LHR',
        # 'AMS',
        # 'BRU',
        # "ZRH",
        # "GVA",
        # "NCE",
        # "CDG",
        # "LYS",
        # "MXP",
        # 'IST',
        ]

    start_dt = '2024-05-25'
    end_dt = '2024-06-10'
>>>>>>> 0fc747b (Update aa/ac usage)
    dates = date_range(start_dt, end_dt)
    number_of_passengers = 2
    airbound_filter = AirBoundFilter(
        max_stops=3,
        airline_include=[],
        airline_exclude=[],
    )
    price_filter = PriceFilter(
        min_quota=1,
        max_miles_per_person=100000,
        preferred_classes=[CabinClass.J, CabinClass.F],
        mixed_cabin_accepted=True
    )
    # seg_sorter = {
    #     'key': 'departure_time',  # only takes 'duration_in_all', 'stops', 'departure_time' and 'arrival_time'.
    #     'ascending': True
    # }

    airbounds = []
    connector = aiohttp.TCPConnector(limit=2)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [SimpleNamespace(ori=ori, des=des, date=date) for ori in origins for des in destinations for date in dates]
        sublists = (tasks[i:i+BATCH_SIZE] for i in range(0, len(tasks), BATCH_SIZE))
        for sublist in sublists:
            acs = Ac_Searcher2()
            airbounds_sublist = await asyncio.gather(*(search_ac(session, acs, airbound.ori, airbound.des, airbound.date, number_of_passengers) for airbound in sublist))
            airbounds.extend([airbound for airbounds_chunks in airbounds_sublist for airbound in airbounds_chunks])

    airbounds = filter_airbounds(airbounds, airbound_filter)
    airbounds = filter_prices(airbounds, price_filter)
    results = []
    for x in airbounds:
        results.extend(x.to_flatted_list())
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


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())