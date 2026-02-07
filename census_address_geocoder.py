import arcpy
import asyncio
import aiohttp
import time
import pandas as pd
import os
from datetime import datetime

# see https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.pdf
BASE_URI = 'https://geocoding.geo.census.gov/geocoder/locations/address'

def build_api_queries(addresses_csv:pd.DataFrame, street_col, city_col, state_col, zip_col, api_key):
    addresses_csv['queries'] = addresses_csv.apply(lambda row: f"{BASE_URI}?street={row[street_col].replace(' ', '+')}&city={row[city_col].replace(' ', '+')}&state={row[state_col].replace(' ', '+')}&zip={row[zip_col].replace(' ', '+')}&benchmark=4&format=json&key={api_key}", axis=1)
    return list(addresses_csv.pop('queries').values)

async def query_census_api(session:aiohttp.ClientSession, query:str, qid:int):
    arcpy.AddMessage(f'INFO @ {datetime.now()}: Initiating API query ({qid}) {query}')
    
    try:
        async with session.get(query) as res:
            start = time.time()
            
            try:
                data = await res.json()
                arcpy.AddMessage(f'    Query {qid} returned JSON response: {data}')
            except Exception as e:
                arcpy.AddError(f'    ERROR @ {datetime.now()}: Error reading response from query ({e})')
                raise e
                
            arcpy.AddMessage(f'    Finished API query ({qid}) in {time.time()-start} seconds')
            return qid, data
    except Exception as e:
        arcpy.AddMessage(f'    ERROR @ {datetime.now()}: Error processing query {qid} ({e})')
        raise e
    
def extract_coords_from_api_res(res):
    coords = (None, None)

    if res['result']['addressMatches']:
        coords = (res['result']['addressMatches'][0]['coordinates']['x'], res['result']['addressMatches'][0]['coordinates']['y'])

    return coords

async def main():
    try:
        addresses_csv = arcpy.GetParameterAsText(0)
        addresses_csv = pd.read_csv(addresses_csv, dtype=str).astype(str)
    except Exception as e:
        arcpy.AddError(f'ERROR @ {datetime.now()}: Error loading addresses CSV ({e})')
        raise e
    
    street_col = arcpy.GetParameterAsText(1)
    city_col = arcpy.GetParameterAsText(2)
    state_col = arcpy.GetParameterAsText(3)
    zip_col = arcpy.GetParameterAsText(4)
    save_to = arcpy.GetParameter(5)
    api_key = arcpy.GetParameter(6)

    queries = build_api_queries(addresses_csv, street_col, city_col, state_col, zip_col, api_key)

    responses = None
    timeout_config = aiohttp.ClientTimeout(total=2000, sock_connect=60, sock_read=300)
    async with aiohttp.ClientSession(timeout=timeout_config) as session:
        tasks = set()
        for i, api_query in enumerate(queries):
            task = asyncio.create_task(query_census_api(session, api_query, i))
            tasks.add(task)
    
        responses = await asyncio.gather(*tasks)
    responses = dict(sorted(dict(responses).items()))
            
    parsed_json_responses = []
    for raw_res in responses.values():
        parsed_json_responses.append(extract_coords_from_api_res(raw_res))

    addresses_csv['X_Coord'] = [coords[0] for coords in parsed_json_responses]
    addresses_csv['Y_Coord'] = [coords[1] for coords in parsed_json_responses]
    
    dest_path = os.path.join(str(save_to), 'geocoded_addresses.csv')
    try:
        addresses_csv.to_csv(dest_path, index=False)
        arcpy.AddMessage(f'INFO @ {datetime.now()}: Saved to {dest_path}')
    except Exception as e:
        arcpy.AddError(f'ERROR @ {datetime.now()}: Error saving CSV ({e})')
    
if __name__ == "__main__":
    asyncio.run(main())