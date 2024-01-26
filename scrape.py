import requests
import pandas as pd
from multiprocessing.pool import ThreadPool
from tqdm import tqdm
import time
import os
from datetime import date, timedelta
from bs4 import BeautifulSoup
from urllib.parse import quote

last_week = date.today() - timedelta(days=7)
# print(quote(last_week.strftime("%m-%d-%Y")))

def set_up_threads(function, list):
    with ThreadPool() as pool:
        results = []
        for result in tqdm(pool.imap(function, list), total=len(list), mininterval=0.5):
            try:
                results.append(result)
            except Exception as e:
                results.append(f"Failed to scrape {item}: {e}")
            time.sleep(0.1) # Add a 100ms delay to slow down the loop
        return results
    

counties = {
    "Dallas": 2057,
    "Denton": 2061,
    "Collin": 2043,
    "Tarrant": 2220,
    "Rockwall": 2199,
    "Kaufman": 2129,
    "Grayson": 2091,
}


def grab_data(url):
    r = requests.get(url)
    return r.json()['data']


def grab_counties(k, v):
    API_endpoint = f'https://www.tdlr.texas.gov/TABS/Search/SearchProjects?length=100&LocationCounty={v}&RegistrationDateBegin={last_week}&start='
    r = requests.get(f'{API_endpoint}0')
    total = r.json()['recordsTotal']
    
    url_list = [f'{API_endpoint}{offset}' for offset in range(0, total, 100)]
    
    data = [x for page in set_up_threads(grab_data, url_list) for x in page]
    
    # print(data)
    df = pd.DataFrame(data)
    print(df)
    if not df.empty:
        df['URL'] = f'https://www.tdlr.texas.gov/TABS/Search/Project/'+df['ProjectNumber']
    df.to_csv(f'data/{k}/{k}_{last_week}.csv', index=False)


def structure_data(text, url):
    project = {}
    keyword_mapping = {
        'Project Name': 'project_name',
        'Project Number': 'project_number',
        'Facility Name': 'facility_name',
        'Location Address': 'location_address',
        'Location County': 'location_county',
        'Start Date': 'start_date',
        'Completion Date': 'completion_date',
        'Estimated Cost': 'est_cost',
        'Type of Work': 'work_type',
        'Type of Funds': 'fund_type',
        'Scope of Work': 'scope',
        'Square Footage': 'sq_ft',
        'Owner Name': 'own_name',
        'Owner Address': 'own_address',
        'Owner Phone': 'own_phone',
        'Tenant Name': 'tenant_name',
        'Design Firm Name': 'design_name',
        'Design Firm Address': 'design_address',
        'Design Firm Phone': 'design_phone'
    }

    for defi in text:
        for k, v in keyword_mapping.items():
            if k in defi.text:
                if v in ['location_address', 'own_address', 'design_address']:
                    project[v] = ", ".join([dd.text.replace('\n', ' ') for dd in defi.find_next_siblings('dd')[:2]])
                else:
                    project[v] = defi.find_next_siblings('dd')[0].text.replace('\n', ' ')
        
    project['project_url'] = url
    
    return project


def grab_detail(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    project = structure_data(soup.find_all('dt'), r.url)
    
    return project


def grab_county_detail(file):
    try:
        df = pd.read_csv(f'{file}.csv')
        projects = df['URL'].unique().tolist()
        data = set_up_threads(grab_detail, projects)
        df = pd.DataFrame(data)
        df.to_csv(f'{file}_detail.csv', index=False)
    except Exception as e:
        print(e)
        

for k, v in counties.items():
    # print(f'data/{k}_{today}.csv')
    grab_counties(k, v)
    grab_county_detail(f'data/{k}/{k}_{last_week}')