import requests
import pandas as pd
from multiprocessing.pool import ThreadPool
from tqdm import tqdm
import time
import os
from datetime import date
from bs4 import BeautifulSoup

today = date.today()

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
    # "Dallas": 2057,
    # "Denton": 2061,
    # "Collin": 2043,
    # "Tarrant": 2220,
    # "Rockwall": 2199,
    # "Kaufman": 2129,
    "Grayson": 2091,
}

def grab_counties():
    for k, v in counties.items():
        print(k, v)
        API_endpoint = f'https://www.tdlr.texas.gov/TABS/Search/SearchProjects?length=100&LocationCounty={v}&start='
        start = 0
        data = []
        url_list = []
        r = requests.get(API_endpoint+str(start))
        total = r.json()['recordsTotal']

        for offset in range(0, total, 100):
            url = f'https://www.tdlr.texas.gov/TABS/Search/SearchProjects?length=100&LocationCounty={v}&start={offset}'
            url_list.append(url)

        def grab_data(url):
            r = requests.get(url)
            data.extend(x for x in r.json()['data'])

        set_up_threads(grab_data, url_list)

        df = pd.DataFrame(data)
        df['URL'] = f'https://www.tdlr.texas.gov/TABS/Search/Project/'+df['ProjectNumber']
        df.to_csv(f'data/{k}/{k}_{today}.csv', index=False)


def structure_data(text, url):
    project = {}
    for defi in text:
        if 'Project Name' in defi.text:
            project['project_name'] = defi.find_next_siblings('dd')[0].text
        if 'Project Number' in defi.text:
            project['project_number'] = defi.find_next_siblings('dd')[0].text
        if 'Facility Name' in defi.text:
            project['facility_name'] = defi.find_next_siblings('dd')[0].text
        if 'Location Address' in defi.text:
            project['location_address'] = ", ".join([dd.text for dd in defi.find_next_siblings('dd')[:2]])
        if 'Location County' in defi.text:
            project['location_county'] = defi.find_next_siblings('dd')[0].text
        if 'Start Date' in defi.text:
            project['start_date'] = defi.find_next_siblings('dd')[0].text
        if 'Completion Date' in defi.text:
            project['completion_date'] = defi.find_next_siblings('dd')[0].text
        if 'Estimated Cost' in defi.text:
            project['est_cost'] = defi.find_next_siblings('dd')[0].text
        if 'Type of Work' in defi.text:
            project['work_type'] = defi.find_next_siblings('dd')[0].text
        if 'Type of Funds' in defi.text:
            project['fund_type'] = defi.find_next_siblings('dd')[0].text
        if 'Scope of Work' in defi.text:
            project['scope'] = defi.find_next_siblings('dd')[0].text
        if 'Square Footage' in defi.text:
            project['sq_ft'] = defi.find_next_siblings('dd')[0].text
        if 'Owner Name' in defi.text:
            project['own_name'] = defi.find_next_siblings('dd')[0].text
        if 'Owner Address' in defi.text:
            project['own_address'] = ", ".join([dd.text for dd in defi.find_next_siblings('dd')[:2]])
        if 'Owner Phone' in defi.text:
            project['own_phone'] = defi.find_next_siblings('dd')[0].text
        if 'Tenant Name' in defi.text:
            project['tenant_name'] = defi.find_next_siblings('dd')[0].text
        if 'Design Firm Name' in defi.text:
            project['design_name'] = defi.find_next_siblings('dd')[0].text
        if 'Design Firm Address' in defi.text:
            project['design_address'] = ", ".join([dd.text for dd in defi.find_next_siblings('dd')[:2]])
        if 'Design Firm Phone' in defi.text:
            project['design_phone'] = defi.find_next_siblings('dd')[0].text
        
    project['project_url'] = url
    return project

def grab_detail(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    project = structure_data(soup.find_all('dt'), r.url)
    
    return project


def grab_county_detail(file):
    df = pd.read_csv(f'{file}.csv')
    projects = df['URL'].unique().tolist()
    data = set_up_threads(grab_detail, projects)
    df = pd.DataFrame(data)
    df.to_csv(f'{file}_detail.csv', index=False)
    
grab_counties()
for k, v in counties.items():
    print(f'data/{k}_{today}.csv')
    grab_county_detail(f'data/{k}/{k}_{today}')