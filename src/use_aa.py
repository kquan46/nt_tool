import time
import asyncio
import aiohttp
from datetime import datetime, timedelta

from aa_searcher import Aa_Searcher
from nt_models import CabinClass, PriceFilter
from nt_parser import results_to_excel, convert_aa_response_to_models
from nt_filter import filter_prices, filter_airbounds, AirBoundFilter
from nt_sorter import get_default_sort_options, sort_airbounds
from utils import date_range


async def main():
    origins = [
        # 'YVR', 'SEA', 'LAX', 'SFO',
        'HKG'
        ]
    destinations = [
        # 'LHR', 'AMS', 'BRU', "ZRH", "GVA", "NCE", "CDG", "LYS", "MXP"
        # 'HKG',
        'YVR', 'SEA', 'LAX', 'SFO'
        ]
    start_dt = '2023-07-01'
    end_dt = '2024-05-26'
    dates = date_range(start_dt, end_dt)
    #  cabin class removed, pls use price filter.
    airbound_filter = AirBoundFilter(
        max_stops=1,
        airline_include=[],
        airline_exclude=[],
    )
    price_filter = PriceFilter(
        min_quota=1,
        max_miles_per_person=999999,
        preferred_classes=[CabinClass.J, CabinClass.F],
        mixed_cabin_accepted=True
    )
    sort_options = get_default_sort_options('Shortest trip')
    aas = Aa_Searcher()
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        airbounds = await asyncio.gather(
            *(search_aa(session, aas, ori, des, date) for ori in origins for des in destinations for date in dates)
        )
        airbounds = [airbound for airbounds_sublist in airbounds for airbound in airbounds_sublist]

    airbounds = filter_airbounds(airbounds, airbound_filter)
    airbounds = filter_prices(airbounds, price_filter)
    airbounds = sort_airbounds(airbounds, sort_options)
    results = []
    for x in airbounds:
        results.extend(x.to_flatted_list())
    result_file_name = f'AA_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_{"-".join(origins)}_to_{"-".join(destinations)}.xlsx'
    results_to_excel(results, out_file_name=result_file_name)

async def search_aa(session, aas, ori, des, date):
    try:
        response = await aas.search_for(session, ori, des, date)
        status = response.status if hasattr(response, "status") else "N/A"
        print(f'search for {ori} to {des} on {date} - {status}')
        if not response.ok:
            print(await response.text())
            await asyncio.sleep(5)
        airbound = await convert_aa_response_to_models(response)
        return airbound
    except Exception as error:
        print(error)
        return list()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())