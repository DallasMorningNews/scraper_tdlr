import requests
import pandas as pd
from multiprocessing.pool import ThreadPool
from tqdm import tqdm
import time

API_endpoint = 'https://www.tdlr.texas.gov/TABS/Search/SearchProjects?length=100&start='
# &LocationCounty=2061
start = 0
data = []
url_list = []

counties = {
    "Dallas": 2047,
    "Denton": 2061,
    "Collin": 2043,
    "Tarrant": 2220,
    "Rockwall": 2199,
    "Kaufman": 2129,
    "Grayson": 2091,
}

r = requests.get(API_endpoint+str(start))
total = r.json()['recordsTotal']

for offset in range(0, total, 100):
    url = f'https://www.tdlr.texas.gov/TABS/Search/SearchProjects?length=100&start={offset}'
    url_list.append(url)


def grab_data(url):
    r = requests.get(url)
    data.extend(x for x in r.json()['data'])


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

print(len(url_list))

start_time = time.time() # record the current time

set_up_threads(grab_data, url_list)
df = pd.DataFrame(data)
print(df.shape)
df.to_csv('tabs.csv', index=False)

end_time = time.time() # record the current time again
elapsed_time = end_time - start_time # calculate the elapsed time

print(f"Elapsed time (sequential): {elapsed_time} seconds")

