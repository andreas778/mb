import urllib.request
import requests
import os
from bs4 import BeautifulSoup
from datetime import datetime
import re
import time
from tqdm import tqdm
import platform
import shutil
import json

def get_today_date_str(dm1='', dm2='', d=True, t=True):
    if d and not(t):
        return datetime.now().strftime('%Y'+dm1+'%m'+dm1+'%d')
    if t and not(d):
        return datetime.now().strftime('%H'+dm2+'%M'+dm2+'%S')
    return datetime.now().strftime('%Y'+dm1+'%m'+dm1+'%d' + '_' + '%H'+dm2+'%M'+dm2+'%S')


download_dir = ''#+download_data_' + get_today_date_str('.') + '/'
#os.makedirs(download_dir, exist_ok=True)
output_dir = '/home/mb/processed_data/'
os.makedirs(output_dir, exist_ok=True)

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

def create_content_dir(arg):
    dir = os.path.join(output_dir, get_today_date_str(dm1='.',t=False) + arg + '-content/')
    os.makedirs(dir, exist_ok=True)
    return dir

def commit_changes(arg, file_name='changes.json'):
    try:
        f = open(file_name)
        changes = json.load(f)
        f.close()
    except:
        changes = {
            'b': 0,
            'f': 0,
            'o': 0,
            'd': 0,
            'w': 0,
            't': 0,
            's': 0,
            'm': 0,
            'n': 0,
            'r': 0,
        }
    finally:
        changes[arg] = int(time.time())    
        f = open(file_name, 'w')
        json.dump(changes, f, indent = 4)
        f.close()


def find_file(file_name, root_dir='.'):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if file_name in filenames:
            return os.path.join(dirpath, file_name)
    return False

def download_file_from_url(url, filename='', check_existing=True):
    filename += url[(url.rfind('/')+1):]
    print('downloading ' +  filename + ' ...')
    if not(check_existing) or not(os.path.isfile(filename)):
        for i in tqdm(range(1)):
            while True:
                try:
                    urllib.request.urlretrieve(url, filename)
                    break
                except:
                    time.sleep(10)
                    continue
    else:
        print('file already exists!')

    return filename


def get_links(url, condition=''):
    # Fetch the HTML content of the page
    print('fetching links ...')
    while True:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status() 
            break
        except Exception as e:
            print(e)
            #print('bad response')
            time.sleep(10)
            continue

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all links that end with .TXT
    #links = [url + a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.TXT')]
    all_links = soup.find_all('a', href=True)

    links = [a['href'] for a in tqdm(all_links) if condition in a['href']]

    return links


def download_tfr_files(url, output_path):
    links = list(set(get_links(url, 'save_pages/detail_')))
    print('downloading TFR files ...')
    flag = False
    for link in tqdm(links):
        digits = re.findall(r'\d+', link)
        link = url[:20] + link[3:14] + digits[0] + '_' + digits[1] + '.shp.zip'
        while True:
            try:
                urllib.request.urlretrieve(link, os.path.join(output_path, link[(link.rfind('/')+1):]))
                break
            except Exception as e:
                if e.code == 404:
                    flag = True
                    break
                time.sleep(10)
                continue
        if flag:
            flag = False
            continue

def delete_previous_content(arg):
    content_dirs = os.listdir(output_dir)
    for dir in content_dirs:
        if len(dir) > 10 and dir[10] == arg:
            shutil.rmtree(os.path.join(output_dir, dir))
            break

def step_t():
    # step -t
    arg = 't'
    print('step (-', arg, ') start ', datetime.now())
    delete_previous_content(arg)
    dir = create_content_dir(arg)
    download_tfr_files("https://tfr.faa.gov/tfr2/list.html",
                        dir)
    print('step (-', arg, ') finish ', datetime.now())
    commit_changes(arg)


def main():
    step_t()

    return

if __name__ == "__main__":
    main()