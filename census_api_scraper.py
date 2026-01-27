import arcpy
import asyncio
import aiohttp
import time
import pandas as pd
import os
from datetime import datetime

# see https://www.census.gov/data/developers/data-sets/acs-5year.html
BASE_URI = 'https://api.census.gov/data/year/acs/acs5?'

# see https://api.census.gov/data/2023/acs/acs5/variables.html
# not sure if these are the same for all ACS years...
ACS_VARS_ALIASES = {
    'B01001_001E': 'Total_Population',
    'B19301_001E': 'Mean_Per_Capita_Income_Yearly',
    'B19013_001E': 'Median_Household_Income_Yearly',
    'B19113_001E': 'Median_Family_Income_Yearly',
    'B19202_001E': 'Median_Nonfamily_Household_Income_Yearly',
    'B08301_002E': 'Total_Car_Van_Truck_Work_Transport',
    'B08301_010E': 'Total_Public_Work_Transport',
    'B08301_011E': 'Total_Public_Bus_Work_Transport',
    'B08301_012E': 'Total_Public_Short_Rail_Work_Transport',
    'B08301_013E': 'Total_Public_Long_Rail_Work_Transport',
    'B08301_015E': 'Total_Public_Ferry_Boat_Work_Transport',
    'B08301_018E': 'Total_Bicycle_Work_Transport',
    'B08301_019E': 'Total_Walked_Work_Transport',
    'B24080_004E': 'Total_Male_Private_Profit_Employee_ClassOfWorker',
    'B24080_005E': 'Total_Male_Private_Profit_SelfEmployedIncorporated_ClassOfWorker',
    'B24080_006E': 'Total_Male_Private_Nonprofit_Employee_ClassOfWorker',
    'B24080_007E': 'Total_Male_Local_Gov_ClassOfWorker',
    'B24080_008E': 'Total_Male_State_Gov_ClassOfWorker',
    'B24080_009E': 'Total_Male_Fed_Gov_ClassOfWorker',
    'B24080_010E': 'Total_Male_SelfEmployedNonIncorporated_ClassOfWorker',
    'B24080_011E': 'Total_Male_UnpaidFamilyWorker_ClassOfWorker',
    'B24080_014E': 'Total_Female_Private_Profit_Employee_ClassOfWorker',
    'B24080_015E': 'Total_Female_Private_Profit_SelfEmployedIncorporated_ClassOfWorker',
    'B24080_016E': 'Total_Female_Private_Nonprofit_Employee_ClassOfWorker',
    'B24080_017E': 'Total_Female_Local_Gov_ClassOfWorker',
    'B24080_018E': 'Total_Female_State_Gov_ClassOfWorker',
    'B24080_019E': 'Total_Female_Fed_Gov_ClassOfWorker',
    'B24080_020E': 'Total_Female_SelfEmployedNonIncorporated_ClassOfWorker',
    'B24080_021E': 'Total_Female_UnpaidFamilyWorker_ClassOfWorker',
}

CENSUS_DATA_PRODUCTS = {
    'population': ['B01001_001E'],
    'mean_per_capita_income_yearly': ['B19301_001E'],
    'median_income_yearly': ['B19013_001E','B19113_001E','B19202_001E'],
    'journey_to_work': ['B08301_002E','B08301_010E','B08301_011E','B08301_012E','B08301_013E','B08301_015E','B08301_018E','B08301_019E'],
    'class_of_worker': [
    'B24080_004E','B24080_005E','B24080_006E','B24080_007E','B24080_008E','B24080_009E',
    'B24080_010E','B24080_011E','B24080_014E','B24080_015E','B24080_016E','B24080_017E',
    'B24080_018E','B24080_019E','B24080_020E','B24080_021E',
    ],
}

def extract_tracts(county_fips, tracts_shp):
    '''
    Extracts census tract IDs from TIGER/Line ShapeFile based on FIPS code (state + county).
    '''
    
    state, county = county_fips[:2], county_fips[2:]
    arcpy.AddMessage(f'INFO @ {datetime.now()}: Extracting census tract IDs for state {state}, county {county}')
    
    tract_ids = []
    
    try:
        fields = ['TRACTCE']
        where_clause = f'STATEFP = \'{state}\' AND COUNTYFP = \'{county}\''
        with arcpy.da.SearchCursor(tracts_shp, fields, where_clause) as cursor:
            for row in cursor:
                tract_id = row[0]
                arcpy.AddMessage(f'    Found tract {tract_id}')
                tract_ids.append(tract_id)
    except Exception as e:
        arcpy.AddError(f'ERROR @ {datetime.now()}: Error while extracting tracts from TIGER/Line ShapeFile ({e})')
        raise e
        
    return tract_ids

def extract_block_groups(county_fips, tract_ids, block_groups_shp) -> dict[int,list[int]]:
    '''
    Extracts census block group IDs from TIGER/Line ShapeFile based on FIPS code (state + county) and census tract IDs.

    MUST supply census tract IDs as block group IDs are not globally unique!
    '''

    state, county = county_fips[:2], county_fips[2:]
    arcpy.AddMessage(f'INFO @ {datetime.now()}: Extracting census block group IDs for state {state}, county {county}')

    block_group_ids: dict[int,list[int]] = {}

    try:
        fields = ['BLKGRPCE']
        for tract_id in tract_ids:
            block_group_ids[tract_id] = []
            where_clause = f'STATEFP = \'{state}\' AND COUNTYFP = \'{county}\' AND TRACTCE = \'{tract_id}\''
            with arcpy.da.SearchCursor(block_groups_shp, fields, where_clause) as cursor:
                for row in cursor:
                    block_group_id = row[0]
                    arcpy.AddMessage(f'    Found block group {block_group_id} in tract {tract_id}')
                    block_group_ids[tract_id].append(block_group_id)
    except Exception as e:
        arcpy.AddError(f'ERROR @ {datetime.now()}: Error while extracting block groups from TIGER/Line ShapeFile ({e})')
        raise e
    
    return block_group_ids
    
def build_api_queries_no_blocks(county_fips, tract_ids, census_data_product, acs_year, api_key) -> list[str]:
    arcpy.AddMessage(f'INFO @ {datetime.now()}: Building census API queries for {census_data_product} (without block groups) ...')
    
    queries = []
    state, county = county_fips[:2], county_fips[2:]
    
    acs_vars = CENSUS_DATA_PRODUCTS[census_data_product]
    
    for tract_id in tract_ids:
        new_uri = BASE_URI.replace('year', str(acs_year))
        
        get_str = 'get=' + ','.join(acs_vars)
        for_str = f'for=tract:{tract_id}'
        in_str = f'in=state:{state}+county:{county}'
        
        queries.append(new_uri + get_str + '&' + for_str + '&' + in_str + '&' + f'key={api_key}')
        arcpy.AddMessage(f'    API query for {census_data_product} data in tract {tract_id}: {queries[-1]}')
        
    arcpy.AddMessage(f'INFO @ {datetime.now()}: Finished building census API queries for {census_data_product}!')
    
    return queries

def build_api_queries_with_blocks(county_fips, block_group_ids:dict[int,list[int]], census_data_product, acs_year, api_key) -> list[str]:
    arcpy.AddMessage(f'INFO @ {datetime.now()}: Building census API queries for {census_data_product} (with block groups) ...')
    
    queries = []
    state, county = county_fips[:2], county_fips[2:]
    
    acs_vars = CENSUS_DATA_PRODUCTS[census_data_product]
    
    for tract_id, bg_ids in block_group_ids.items():
        new_uri = BASE_URI.replace('year', str(acs_year))

        get_str = 'get=' + ','.join(acs_vars)
        in_str = f'in=state:{state}+county:{county}+tract:{tract_id}'

        for bg_id in bg_ids:
            for_str = f'for=block%20group:{bg_id}' # see https://api.census.gov/data/2020/pdb/blockgroup/examples.html
            queries.append(new_uri + get_str + '&' + for_str + '&' + in_str + '&' + f'key={api_key}')
            arcpy.AddMessage(f'    API query for {census_data_product} data in tract {tract_id}: {queries[-1]}')

    arcpy.AddMessage(f'INFO @ {datetime.now()}: Finished building census API queries for {census_data_product}!')

    return queries

async def query_census_api(session:aiohttp.ClientSession, query:str):
    arcpy.AddMessage(f'INFO @ {datetime.now()}: Initiating API query {query}')
    
    try:
        async with session.get(query) as res:
            start = time.time()
            
            try:
                data = await res.json()
                arcpy.AddMessage(f'    Query returned JSON response: {data}')
            except Exception as e:
                arcpy.AddError(f'    ERROR @ {datetime.now()}: Error reading response from query ({e})')
                raise e
                
            arcpy.AddMessage(f'    Finished API query in {time.time()-start} seconds')
            return data
    except Exception as e:
        arcpy.AddMessage(f'    ERROR @ {datetime.now()}: Error processing query ({e})')
        raise e
        
def parse_census_api_json_res(res, use_aliases=False):
    arcpy.AddMessage(f'INFO @ {datetime.now()}: Parsing census API response {res}')
    d = {}
    
    for i in range(len(res[0])):
        var_id, val = str(res[0][i]), str(res[1][i])
        if not use_aliases or var_id not in ACS_VARS_ALIASES:
            d[var_id] = val
        else:
            var_alias = ACS_VARS_ALIASES[var_id]
            d[var_alias] = val
        
    arcpy.AddMessage(f'    Parsed response: {d}')
    return d
    
async def main():
    county_fips = arcpy.GetParameterAsText(0)
    census_data_product = arcpy.GetParameter(1)
    acs_year = arcpy.GetParameter(2)
    tracts_shp = arcpy.GetParameter(3)
    api_key = arcpy.GetParameter(4)
    save_to = arcpy.GetParameter(5)
    use_aliases = arcpy.GetParameter(7)
    total_timeout = arcpy.GetParameter(8)
    
    if len(county_fips) != 5:
        arcpy.AddError(f'ERROR @ {datetime.now()}: FIPS codes must be 5 digits - got {len(county_fips)} instead')
        raise Exception('Invalid FIPS code')
        
    tract_ids = extract_tracts(county_fips, tracts_shp)
    arcpy.AddMessage(f'INFO @ {datetime.now()}: Found {len(tract_ids)} tracts in {county_fips}')

    if arcpy.GetParameterAsText(6) == '' or arcpy.GetParameterAsText(6) is None:
        block_group_ids = None
    else:
        block_groups_shp = arcpy.GetParameter(6)
        block_group_ids = extract_block_groups(county_fips, tract_ids, block_groups_shp)

    if block_group_ids is None:
        api_queries = build_api_queries_no_blocks(county_fips, tract_ids, census_data_product, acs_year, api_key)
    else:
        api_queries = build_api_queries_with_blocks(county_fips, block_group_ids, census_data_product, acs_year, api_key)
    
    raw_json_responses = []
    timeout_config = aiohttp.ClientTimeout(total=total_timeout, sock_connect=60, sock_read=300)
    async with aiohttp.ClientSession(timeout=timeout_config) as session:
        tasks = set()
        for _, api_query in enumerate(api_queries):
            task = asyncio.create_task(query_census_api(session, api_query))
            tasks.add(task)
    
        for future in asyncio.as_completed(tasks):
            json_res = await future
            raw_json_responses.append(json_res)
            
    parsed_json_responses = []
    for raw_res in raw_json_responses:
        parsed_json_responses.append(parse_census_api_json_res(raw_res, use_aliases=use_aliases))
        
    df = pd.DataFrame(parsed_json_responses)
    dest_path = os.path.join(str(save_to), ('tracts_' if block_group_ids is None else 'blockgroups_')+f'acs5_{county_fips}_{census_data_product}_{acs_year}.csv')
    try:
        df.to_csv(dest_path, index=False)
        arcpy.AddMessage(f'INFO @ {datetime.now()}: Saved to {dest_path}')
    except Exception as e:
        arcpy.AddError(f'ERROR @ {datetime.now()}: Error saving CSV ({e})')
    
if __name__ == "__main__":
    asyncio.run(main())