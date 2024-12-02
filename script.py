import urllib.request
import requests
import zipfile
import os
from bs4 import BeautifulSoup
from datetime import datetime
import csv
import pandas as pd
import re
import time
from tqdm import tqdm
from alive_progress import alive_bar
import argparse
import shutil
import platform
import copy
import gzip
import json
import calendar
import fitz
import psycopg2
import geopandas as gpd
from PIL import Image, ImageChops
import numpy as np


def get_today_date_str(dm1='', dm2='', d=True, t=True):
    if d and not(t):
        return datetime.now().strftime('%Y'+dm1+'%m'+dm1+'%d')
    if t and not(d):
        return datetime.now().strftime('%H'+dm2+'%M'+dm2+'%S')
    return datetime.now().strftime('%Y'+dm1+'%m'+dm1+'%d' + '_' + '%H'+dm2+'%M'+dm2+'%S')


download_dir = 'downloaded_data/'    #+download_data_' + get_today_date_str('.') + '/'
os.makedirs(download_dir, exist_ok=True)
if platform.system() == "Windows":
    output_dir = 'home/'
else:
    output_dir = '/home/mb/processed_data/'
os.makedirs(output_dir, exist_ok=True)

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

base_airpot_url = "https://airnav.com/airport/"
data_link = 'https://www.faa.gov/air_traffic/flight_info/aeronav/aero_data/NASR_Subscription/'

def create_content_dir(arg):
    dir = os.path.join(output_dir, get_today_date_str(dm1='.',t=False) + arg + '-content/')
    os.makedirs(dir, exist_ok=True)
    return dir

def find_file(file_name, root_dir='.'):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if file_name in filenames:
            return os.path.join(dirpath, file_name)
    return False


f = open('db_config.json')
db_params = json.load(f)
f.close()

def get_value_type(val):
    try:
        int(val)
        if val < 2147483647:
            return ['INTEGER', 0]
        else:
            return ['BIGINT', 0]
    except:
        try:
            float(val)
            return ['REAL', 1]
        except:
            try:
                dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                return ['TIMESTAMP', 2]
            except:
                if val.lower() == 'true' or val.lower() == 'false':
                    return ['BOOLEAN', 3]
                else:
                    return ['TEXT', 4]
                
def get_unique_headers(headers):
    for i in range(len(headers)):
        count = 0
        for j in range(i+1, len(headers)):
            if headers[i] == headers[j]:
                count += 1
                headers[j] += '_' + str(count)


def create_table_from_csv(table_name, csv_file_path):
    res = f"CREATE TABLE {table_name} ("
    with open(csv_file_path, 'r', encoding="utf-8") as csv_file:
        f = csv.reader(csv_file)
        for row in f:
            headers = row
            break
        get_unique_headers(headers)
        for i in tqdm(range(len(headers))):
            res += headers[i] + ' '
            csv_file.seek(0)
            value_type = ['', -1]
            skip_first = True
            for row in f:
                if skip_first:
                    skip_first = False
                    continue
                if row[i]:
                    tmp_value_type = get_value_type(row[i].strip())
                    if value_type[1] < tmp_value_type[1]:
                        value_type = tmp_value_type
                if value_type[0] == 'TEXT':
                    break
            if not(value_type[0]):
                value_type[0] = 'TEXT'
            res += value_type[0] + ','

    res = res[:(res.rfind(','))] + res[(res.rfind(',')+1):] + ");"
    #print(res)
    return res


def equal_columns(input_file):
    with open(input_file, 'r') as infile:
        reader = csv.reader(infile)
        
        # Read all rows and find the longest row
        rows = list(reader)
        max_columns = max(len(row) for row in rows)
        
        # Adjust headers to match the longest row
        headers = rows[0]
        if len(headers) < max_columns:
            headers.extend(f"extra_col_{i+1}" for i in range(len(headers), max_columns))
        
    # Write updated CSV to the output file
    with open(input_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(headers)  # Write adjusted headers
        writer.writerows(rows[1:])  # Write the rest of the data

def update_table_from_csv(table_name, csv_file_path, column_config=''):
    print('addding content to the database ...')
    while True:
        try:
            conn = psycopg2.connect(**db_params)
            break
        except:
            time.sleep(5)

    cur = conn.cursor()
    conn.autocommit = True

    try:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    except Exception as e:
        print(e)
        conn.commit()
    finally:
        try:
            equal_columns(csv_file_path)
            exec_str = create_table_from_csv(table_name, csv_file_path)
            #print('HERE IS EXEC STR', exec_str)
            cur.execute(exec_str)
        except Exception as e:
            print(e)
            conn.commit()
        finally:
            try:
                cur.execute(f"COPY {table_name} FROM '{csv_file_path}' WITH CSV HEADER NULL '';")
                if conn:
                    conn.commit()
                    cur.close()
                    conn.close()
            except Exception as e:
                print(e)
                cur.close()
                conn.close()



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


def download_file_from_url(url, filename='', check_existing=True):
    filename = os.path.join(filename, url[(url.rfind('/')+1):])
    print('downloading ' +  filename + ' ...')
    if not(check_existing) or not(os.path.isfile(filename)):
        for i in tqdm(range(1)):
            t = 10
            while True:
                try:
                    res = urllib.request.urlretrieve(url, filename)
                    break
                except:
                    time.sleep(t)
                    t += 10
                    continue
    else:
        print('file already exists!')

    return filename

def extract_file_from_nested_zip(outer_zip_path, inner_zip_name, file_to_extract, output_dir):
    # Step 1: Open the outer zip file
    print('extracting ' + file_to_extract + ' ...')
    for i in tqdm(range(1)):
        with zipfile.ZipFile(outer_zip_path, 'r') as outer_zip:
            # Step 2: Open the inner zip file directly from the outer zip
            with outer_zip.open(inner_zip_name) as inner_zip_file:
                with zipfile.ZipFile(inner_zip_file) as inner_zip:
                    # Step 3: Extract the specific file from the inner zip
                    with inner_zip.open(file_to_extract) as target_file:                  
                        # Write the contents to the output file
                        output_file_path = os.path.join(output_dir, file_to_extract)
                        with open(output_file_path, 'wb') as output_file:
                            output_file.write(target_file.read())
    return output_file_path
    

def get_links(url, condition=''):
    # Fetch the HTML content of the page
    print('fetching links ...')
    t = 10
    while True:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status() 
            break
        except Exception as e:
            #print(e)
            #print('bad response')
            time.sleep(t)
            t += 10
            continue

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all links that end with .TXT
    #links = [url + a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.TXT')]
    all_links = soup.find_all('a', href=True)

    links = [a['href'] for a in tqdm(all_links) if condition in a['href']]

    return links
    
def process_text_for_wx_file(text):
    try:
        datetime_object = datetime.strptime(text[:text.find('\n')]+':00', '%Y/%m/%d %H:%M:%S')
        if datetime_object.hour < 12:
            datetime_object = str(datetime_object).replace(' ', ', ') + ' AM'
        else:
            datetime_object = str(datetime_object).replace(' ', ', ') + ' PM'
        text = datetime_object + ', ' + text[(text.find('\n')+1):].replace(' ', ', ')
    except:
        pass
    finally:
        return text[:((re.search(r'([a-zA-Z])([^a-zA-Z]*)$', text)).end(1))] + '\n'


def create_wx_file(url, output_file, base_file):
    metar_gz = download_file_from_url(url, download_dir, check_existing=False)
    print('creating  wx_file.csv ...')
    with gzip.open(metar_gz, 'rb') as f_in:
        with open(metar_gz[:-3], 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    f = open(metar_gz[:-3], 'r')
    g = open(output_file, 'w')
    f_lines = f.readlines()
    i = 0
    for line in tqdm(f_lines):
        if i < 5:
            i += 1
            continue
        g.write(line)
    f.close()
    g.close()

    print('processing wx_file.csv ...')
    output_rows = []
    with open(output_file, 'r', encoding="utf-8") as csv_file:
        f = csv.reader(csv_file)
        row_count = sum(1 for row in f)
        csv_file.seek(0)
        skip_first = True
        with alive_bar(row_count) as bar:
            for row in f:
                res_row = row
                res_row.insert(2, '')
                if skip_first:
                    skip_first = False
                else:
                    if row[1][0] == 'K':
                        with open(base_file, 'r', encoding="utf-8") as csv_file2:
                            g = csv.reader(csv_file2)
                            for row2 in g:
                                if row2[4] == row[1][1:]:
                                    res_row[2] = row2[5]
                                    break
                output_rows.append(res_row)
                bar()

    output_rows[0][2] = 'city'

    with open(output_file, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(output_rows)




def get_airport_urls(base_url, file_name_of_extracted_file):
    print('getting Airport urls ...')

    airport_urls = []

    with open(file_name_of_extracted_file, 'r', encoding="utf-8") as csv_file:
        f = csv.reader(csv_file)
        row_count = sum(1 for row in f)
        csv_file.seek(0)
        with alive_bar(row_count) as bar:
            for row in f:
                airport_url = base_url + row[4]
                airport_urls.append(airport_url)
                bar()
    
    del airport_urls[0]
    
    return airport_urls

def find_nth(haystack: str, needle: str, n: int) -> int:
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start

def get_start_ind(tpa_data, tpa_ind):
    ind1 = tpa_data.rfind('.', 0, tpa_ind)
    #ind2 = tpa_data.rfind('\n', 0, tpa_ind)
    if ind1 != -1:
        return ind1+1
    else:
        return 0

def get_end_ind(tpa_data):
    ind1 = tpa_data.find('.')
    #ind2 = tpa_data.find('\n')
    return ind1

def get_tpa(arpt_data, data):
    tpa = []
    flag = False
    tpa_ind = find_nth(arpt_data, 'TPA', 2)
    if tpa_ind == -1:
        tpa_ind = find_nth(arpt_data, 'TPA', 1)
        if tpa_ind != -1:
            tpa_data = arpt_data[tpa_ind:]
            tpa_string = tpa_data[get_start_ind(tpa_data, tpa_ind):get_end_ind(tpa_data)]
            if tpa_string.find('See') == -1:
                digits = re.findall(r'\d+', tpa_string)
                if digits:
                    for d in digits:
                        if int(d) >= 300:
                            tpa = [d]
                            break
            else:
                ind = tpa_data.lower().find('traffic pattern') #fetch entire sentence before and after
                if ind != -1:
                    #rind = tpa_data[:ind].rfind('.')
                    #if rind != -1:
                        #ind = rind
                    tpa_data = tpa_data[get_start_ind(arpt_data, ind):]
                    flag = True
        else:
            ind = arpt_data.lower().find('traffic pattern')
            if ind != - 1:
                tpa_data = arpt_data[get_start_ind(arpt_data, ind):]
                flag = True
    else:
        tpa_data = arpt_data[get_start_ind(arpt_data, tpa_ind):]
        flag = True

    if flag:
        tpa_string = tpa_data[:get_end_ind(tpa_data)]
        if tpa_string.find('Rwy') == -1:
            digits = re.findall(r'\d+', tpa_string)
            if digits:
                for d in digits:
                    if int(d) >= 300:
                        tpa = [d]
                        break
        else:
            tpa_strings = tpa_string.split(',')
            for t in tpa_strings:
                digits = re.findall(r'\d+', t)
                if digits:
                    for d in digits:
                        if int(d) >= 300:
                            for _ in range(int(len(data)/2)):
                                tpa.append(d)
                            break

    if not tpa:
        tpa.append('')

    i = 0
    for key in data:
        data[key]['TPA'] = tpa[i]
        if i < len(tpa)-1:
            i += 1

    return tpa
                        

def process_arpt_data(arpt_data):
    
    data = get_rgt(arpt_data)

    get_tpa(arpt_data, data)

    return data
            

def get_rgt(arpt_data):
    rgt = {}
    rwy_ind = arpt_data.find('\nRWY')
    while rwy_ind != -1:
        rwy_end_ind = arpt_data.find('\n', rwy_ind+1)
        if arpt_data.find('-', rwy_ind, rwy_ind+10) == -1 and arpt_data.find(':', rwy_ind, rwy_ind+10) != -1:
            rwy_string = arpt_data[rwy_ind:rwy_end_ind]
            rwy = rwy_string[(rwy_string.find(' ')+1):rwy_string.find(':')]
            if rwy_string.find('Rgt tfc') != -1:
                tfc = 'R'
            else:
                tfc = ''
            if rgt.get(rwy, -1) == -1:
                rgt[rwy] = {}
                rgt[rwy]['Rgt'] = tfc
        rwy_ind = arpt_data.find('\nRWY', rwy_end_ind)

    return rgt


def process_pdfs(pdfs, arpt_id):
    arpt_ind = '(' + arpt_id + ')'
    for pdf in pdfs:
        with fitz.open(os.path.join(download_dir, pdf)) as doc:
            flag = False
            text = ''
            for page in doc:
                text += page.get_text()
                if not flag:
                    arpt_start = text.find(arpt_ind)
                if arpt_start != -1:
                    arpt_end = find_nth(text[arpt_start:], 'UTC', 2)
                    if arpt_end != -1 or flag:
                        arpt_data = text[arpt_start:arpt_end]
                        return process_arpt_data(arpt_data)
                    else:
                        flag = True
                else:
                    text = ''
                    
    return [[''], ['']]


def get_arpt_data(text):
    ind = find_nth(text, 'UTC', 2)
    return ind  # text[:ind]


def extracting_pdf_info(pdfs, arpts_list):
    print('extracting pdf data ...')
    arpts_dict = {}
    for pdf in tqdm(pdfs):
        skip = 0
        flag = False
        with fitz.open(os.path.join(download_dir, pdf)) as doc:
            text = ''
            for page in tqdm(doc):
                if skip < 30:
                    skip += 1
                    continue
                text = page.get_text()
                brace_ind = 0
                while brace_ind != -1:
                    arpt_data = False
                    brace_ind = text.find('(', brace_ind)
                    if brace_ind != -1:
                        if text[brace_ind + 4] == ')':
                            arpt_id = text[(brace_ind+1):(brace_ind+4)]
                            if arpt_id in arpts_list:
                                arpt_data = get_arpt_data(text[(brace_ind+1):]) 
                                #if arpt_id == 'HWD':
                                    #x=0
                        elif text[brace_ind + 5] == ')':
                            arpt_id = text[(brace_ind+1):(brace_ind+5)]
                            if arpt_id in arpts_list:
                                arpt_data = get_arpt_data(text[(brace_ind+1):])
                        if arpt_data:
                            arpt_data = text[brace_ind:arpt_data]
                            if 'UTC' in arpt_data:
                                arpts_dict[arpt_id] = process_arpt_data(arpt_data)
                        brace_ind += 5
    return arpts_dict



def get_values_list_from_csv(csv_file, column):
    df = pd.read_csv(csv_file)
    return df[column].tolist()


def create_base_file(input_file, input_file2, input_file3, input_file4, input_pdf, output_file, base_url):
    output_fields = ['Identifier','City','State','Country','Lat','Long','Elevation','CTAF',
                     'UNICOM','ATIS', 'AWOS','ASOS','GROUND','TOWER', 'TOWER2',
                     'CLEARANCE DELIVERY', 'RWY_ID', 'TPA', 'Rgt_tfc', 'RWY_LEN', 'RWY_WIDTH']
    
    output_rows = []

    airport_urls = []

    max_runways = 1

    arpts_list = get_values_list_from_csv(input_file, 'ARPT_ID')
    arpts_dict = extracting_pdf_info(input_pdf, arpts_list)
    
    #f = open('arpts_dict.json', 'r')
    #arpts_dict = json.load(f)
    #f.close()

    f = open('arpts_dict.json', 'w')
    json.dump(arpts_dict, f, indent=4)
    f.close()

    print('creating  base_file.csv ...')
    with open(input_file, 'r', encoding="utf-8") as csv_file:
        f = csv.reader(csv_file)
        #skip_first = True
        row_count = sum(1 for row in f)
        csv_file.seek(0)
        skip_first = True
        with alive_bar(row_count) as bar:
            for row in f:
                if skip_first:
                    skip_first = False
                    continue

                airport_url = base_url + row[4]
                airport_urls.append(airport_url)
                last_fields = get_last_fields(input_file2, row[4])
                
                with open(input_file4, 'r', encoding="utf-8") as csv_file4:
                    fg = csv.reader(csv_file4)
                    for row4 in fg:
                        if row4[1] == row[4]:
                            if row4[1]:
                                last_fields[3] += ' ' + row4[20]
                            else:
                                last_fields[3] += row4[20]
                            break

                with open(input_file3, 'r', encoding="utf-8") as csv_file3:
                    h = csv.reader(csv_file3)
                    runways = []
                    count_runways = 0
                    flag = False
                    for row3 in h:
                        if row3[4] == row[4]:
                            runways.append(row3[7])
                            runways.append(row3[8])
                            runways.append(row3[9])
                            count_runways += 1
                            flag = True
                            if count_runways >= 10:
                                break
                        else:
                            if flag:
                                break
                    if count_runways > max_runways:
                        max_runways = count_runways

                #pdf_data = process_pdfs(input_pdf, row[4])
                #print('pdf_data = ', pdf_data, row[4])
                #tpa = pdf_data[0]
                #rgt = pdf_data[1]

                output_row = [row[4], row[5], row[3], row[6], row[19], row[24], row[26]]

                output_row.extend(last_fields)
                

                if arpts_dict.get(row[4], 0):
                    i = 0
                    while i+2 < len(runways):
                        rgt = ''
                        tpa = ''
                        rwy_id = runways[i].split('/')
                        for rwy in arpts_dict[row[4]]:
                            if rwy == rwy_id[0] or rwy == rwy_id[-1]:
                                if arpts_dict[row[4]][rwy]['Rgt'] == 'R':
                                    if rwy[-1] != 'R':
                                        rgt = rwy + 'R'
                                    else:
                                        rgt = rwy
                                tpa = arpts_dict[row[4]][rwy]['TPA']
                        output_row.extend([runways[i], tpa, rgt, runways[i+1], runways[i+2]])
                        i += 3
                else:
                    i = 0
                    while i+2 < len(runways):
                        output_row.extend([runways[i], '', '', runways[i+1], runways[i+2]])
                        i += 2

                #output_row.extend(runways)
                #if max_row_len < len(output_row): max_row_len = len(output_row)
                output_rows.append(copy.copy(output_row))
                #if max_runways == 3: break
                bar()

    if max_runways > 10:
        max_runways = 10

    for i in range(1, max_runways+1):
        output_fields.extend(['RWY_ID_' + str(i), 'TPA_' + str(i),
                              'Rgt_tfc_' + str(i), 'RWY_LEN_' + str(i),
                              'RWY_WIDTH_' + str(i)
                              ])
        
    #del output_rows[0]

    for row in output_rows:
        if len(row) < len(output_fields):
            row.extend([''] * (len(output_fields) - len(row)))

    with open(output_file, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(output_fields)
        csvwriter.writerows(output_rows)

    return airport_urls



def get_last_fields(input_file, arpt_id):      
    with open(input_file, 'r', encoding="utf-8") as csv_file3:
        h = csv.reader(csv_file3)
        values = [''] * 9
        flag = False
        for row in h:
            if row[1] == arpt_id:
                flag = True
                if row[19].find('CTAF') != -1:
                    values[0] += row[17] + ' '
                elif row[19].find('UNICOM') != -1:
                    values[1] += row[17] + ' '
                elif row[19].find('ATIS') != -1:
                    values[2] += row[17] + ' '
                elif row[19].find('AWOS') != -1:
                    values[3] += row[17] + ' '
                elif row[19].find('ASOS') != -1:
                    values[4] += row[17] + ' '
                elif row[19].find('GND/P') != -1:
                    values[5] += row[17] + ' '
                elif row[19].find('LCL/P') != -1:
                    values[6] += row[17] + ' '
                elif row[19].find('LCL/S') != -1:
                    values[7] += row[17] + ' '
                elif row[19].find('CD/P') != -1:
                    values[8] += row[17] + ' '
            else:
                if flag:
                    break

    for i in range(len(values)):
        if values[i]:
            if values[i][-1] == ' ':
                values[i] = values[i][:-1]

    return values


        
def create_waypoint_file(input_file, output_file):
    print('creating  waypoint file.csv ...')
    output_fields = ['FIX_ID','STATE_CODE','COUNTRY_CODE','LAT_DECIMAL','LONG_DECIMAL', 'FIX_USE_CODE']
    
    output_rows = []

    with open(input_file, 'r', encoding="utf-8") as csv_file:
        f = csv.reader(csv_file)
        row_count = sum(1 for row in f)
        csv_file.seek(0)
        with alive_bar(row_count) as bar:
            for row in f:
                if row[17].strip() == 'RP' or row[17].strip() == 'VFR':
                    output_rows.append([row[1], row[3], row[4], row[9], row[14], row[17].strip()])
                bar()

    del output_rows[0]

    with open(output_file, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(output_fields)
        csvwriter.writerows(output_rows)


        
def create_nav_file(input_file, output_file):
    print('creating  nav.csv ...')
    output_fields = ['NAV_ID','NAV_TYPE','STATE_CODE','CITY','LAT_DECIMAL', 'LONG_DECIMAL', 'FREQ']
    
    output_rows = []

    with open(input_file, 'r', encoding="utf-8") as csv_file:
        f = csv.reader(csv_file)
        row_count = sum(1 for row in f)
        csv_file.seek(0)
        with alive_bar(row_count) as bar:
            for row in f:
                output_rows.append([row[1], row[2], row[3], row[4], row[26], row[31], row[54]])
                bar()

    del output_rows[0]

    with open(output_file, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(output_fields)
        csvwriter.writerows(output_rows)
       


def create_rwy_end(input_file, output_file):
    print('creating  RWY_END.csv ...')
    output_fields = ['ARPT_ID','RWY_ID','LAT_DECIMAL','LONG_DECIMAL','LAT_DECIMAL_END','LONG_DECIMAL_END']
    
    output_rows = []

    with open(input_file, 'r', encoding="utf-8") as csv_file:
        f = csv.reader(csv_file)
        row_count = sum(1 for row in f)
        csv_file.seek(0)
        with alive_bar(row_count) as bar:
            for row in f:
                if output_rows and row[4] == output_rows[-1][0] and row[7] == output_rows[-1][1]:
                    #output_rows[-1].extend([row[18], row[23]])
                    output_rows[-1][-2] = row[18]
                    output_rows[-1][-1] = row[23]
                    continue
                output_rows.append([row[4], row[7], row[18], row[23], '', ''])
                bar()
                
    del output_rows[0]

    with open(output_file, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(output_fields)
        csvwriter.writerows(output_rows)
       


def create_daily_obstacle_file(input_file, output_file):
    print('creating  DDOF_file.csv ...')
    output_fields = ['COUNTRY','STATE','CITY','LATDEC','LONDEC','TYPE','AMSL']
    
    output_rows = []

    with open(input_file, 'r', encoding="utf-8") as csv_file:
        f = csv.reader(csv_file)
        row_count = sum(1 for row in f)
        csv_file.seek(0)
        with alive_bar(row_count) as bar:
            for row in f:
                output_rows.append([row[2].strip(), row[3].strip(), row[4].strip(), row[5].strip(),
                                    row[6].strip(), row[9].strip(), row[12].strip()])
                bar()
                
    del output_rows[0]

    with open(output_file, 'w', newline='') as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        # writing the fields
        csvwriter.writerow(output_fields)
        # writing the data rows
        csvwriter.writerows(output_rows)



def create_obstacle_file(input_files, input_dir, output_file):
    #print('creating obstacle file.csv ...')
    print('processing .DAT files ...')
    
    # Define the column widths based on the observed pattern
    column_widths = [
        10,  # OAS#
        2,   # V
        3,   # CO
        3,   # ST
        17,  # CITY
        3,  # LATITUDE DEG
        3,  # LATITUDE MIN
        7,  # LATITUDE SEC
        4,  # LONGITUDE DEG
        3,  # LONGITUDE MIN
        7,  # LONGITUDE SEC
        19,  # OBSTACLE TYPE
        2,  #
        6,   # AGL HT
        6,   # AMSL HT
    ]

    # Define the column names based on the header
    column_names = [
        "OAS#", "V", "COUNTRY", "STATE", "CITY", "LATITUDE DEG", "LATITUDE MIN", "LATITUDE SEC", "LONGITUDE DEG",
        "LONGITUDE MIN", "LONGITUDE SEC", "OBSTACLE TYPE", "0", "AGL HT", "AMSL"
    ]
    
    data = []
    for input_file in tqdm(input_files):
        if input_file.find('.Dat') == -1:
            continue
        # Read the file contents
        with open(os.path.join(input_dir, input_file), 'r', encoding="utf-8") as file:
            lines = file.readlines()

        # Skip the first few lines and extract the data part
        data_lines = lines[4:]

        # Parse the data using fixed column widths
        #print('processing  obstacle file.csv ...')
        #data = []
        #for line in tqdm(data_lines):
        for line in data_lines:
            row = []
            start = 0
            for width in column_widths:
                end = start + width
                row.append(line[start:end].strip())
                start = end
            data.append(row)

    # Create a DataFrame
    df = pd.DataFrame(data, columns=column_names)

    lat_dec = []
    long_dec = []
    
    print('creating  obstacle file.csv ...')
    with alive_bar(len(data)) as bar:
        for index, row in df.iterrows():
            dec = float(row["LATITUDE DEG"]) + float(row["LATITUDE MIN"])/60 + float(row["LATITUDE SEC"][:-1])/3600
            if row["LATITUDE SEC"][-1] == 'S':
                dec = -dec
            lat_dec.append(round(dec, 6))
            dec = float(row["LONGITUDE DEG"]) + float(row["LONGITUDE MIN"])/60 + float(row["LONGITUDE SEC"][:-1])/3600
            if row["LONGITUDE SEC"][-1] == 'W':
                dec = -dec
            long_dec.append(round(dec, 6))
            bar()

    df.insert(5, "LATDEC", lat_dec)
    df.insert(6, "LONGDEC", long_dec)

    columns = ["OAS#", "V", "LATITUDE DEG", "LATITUDE MIN", "LATITUDE SEC", "LONGITUDE DEG",
                "LONGITUDE MIN", "LONGITUDE SEC", "0", "AGL HT"]
    df.drop(columns, inplace=True, axis=1)
    df.to_csv(output_file, index=False)



def extract_single_file(path, file_to_extract, output_path):
    print('extracting  ' + file_to_extract + ' ...')
    for i in tqdm(range(1)):
        with zipfile.ZipFile(path, 'r') as zip:
            zip.extract(file_to_extract, output_path)
    return os.path.join(output_path, file_to_extract)

def extract_all_files(path, output_path='', bprint=True):
    if bprint:
        print('extracting  ' + path + ' ...') 
        for i in tqdm(range(1)):
            with zipfile.ZipFile(path, 'r') as zip:
                zip.extractall(output_path)
    else:
        with zipfile.ZipFile(path, 'r') as zip:
            zip.extractall(output_path)
    return zip.namelist()


def convert_shp_to_csv(files_date_list, download_dir, output_path):
    print('converting shp to csv ...')
    files_list = files_date_list[0]
    ind = 0
    for file in tqdm(files_list):
        with zipfile.ZipFile(file, 'r') as zip:
            files = extract_all_files(file, download_dir, False)
            for f in files:
                if f.find('.shp') != -1:
                    file_name = os.path.join(download_dir, f)
                    geopandas_df = gpd.read_file(file_name)
                    converted_df = geopandas_df.to_crs('EPSG:4326')
                    converted_df['EFFECTIVE'] = files_date_list[1][ind]
                    ind += 1
                    converted_df.to_csv(os.path.join(output_path, f.replace('.shp', '.csv')), index=False)
                    """myshpfile = gpd.read_file(file_name)
                    myshpfile.to_file(os.path.join(output_path, f.replace('.shp', '.geojson')), driver='GeoJSON')"""
                    break


def create_class_airspace(shape_file_path, dir):
    print('creating Class_Airspace.csv ...')
    for _ in tqdm(range(1)):
        file_name = os.path.join(download_dir, shape_file_path)

        geopandas_df = gpd.read_file(file_name)
        converted_df = geopandas_df.to_crs('EPSG:4326')
        file_name = 'Class_Airspace'
        file_path = os.path.join(download_dir, file_name + '.csv')

        converted_df.to_csv(file_path, index=False)

        columns = ['IDENT', 'NAME', 'UPPER_VAL', 'UPPER_CODE', 'LOWER_VAL', 'LOWER_CODE', 'CLASS', 'SECTOR',
                   'SHAPE_Leng', 'SHAPE_Area', 'geometry']
        
        df = pd.read_csv(file_path)
        df.drop(df.columns.difference(columns), inplace=True, axis=1)
        df = df.reset_index()  
        indexClass = df[ (df['CLASS'] != 'B') & (df['CLASS'] != 'C') & (df['CLASS'] != 'D') ].index
        df.drop(indexClass , inplace=True)
        df.to_csv(file_path, index=False) 

        simplify_class(file_path, dir)

    return file_path


def geometry_array(loc, data, key):
    polygonval = data[loc][key]
    polyArray = polygonval[polygonval.find("(((")+3:polygonval.find(")))")]
    polyarraylist= polyArray.split(',') # converted to array
    return polyarraylist

def coord_variation(coords, tollerance):
    coodlength=len(coords)
    scanspot=coodlength-2  # the last item in the array should not be touched
    affected_coord = 0
    geom_text="MULTIPOLYGON Z (((" + coords[0] + ","     # used to format the output similar to the original file
    geom_last = coords[coodlength-1]              # the last pont in a polygon should be maintained for a closed polygon to be formed
    while (scanspot > 0):           # effectivly excludes the first index
        proceed = True
    
        if coords[scanspot].startswith('(') or coords[scanspot-1].startswith('(') or coords[scanspot + 1].startswith('(') or coords[scanspot].endswith(')') or coords[scanspot-1].endswith(')') or coords[scanspot + 1].endswith(')'):
            proceed = False
        else:
            proceed = True
            
        if proceed:
            #print (coords[scanspot].split()[0])       
            lat=float(coords[scanspot].split()[0].replace('(', '').replace(')', ''))
            lon=float(coords[scanspot].split()[1].replace('(', '').replace(')', ''))
            next_lat=float(coords[scanspot+1].split()[0].replace('(', '').replace(')', ''))
            next_lon=float(coords[scanspot+1].split()[1].replace('(', '').replace(')', ''))
            prev_lat=float(coords[scanspot-1].split()[0].replace('(', '').replace(')', ''))
            prev_lon=float(coords[scanspot-1].split()[1].replace('(', '').replace(')', ''))
            lat_diff= abs(next_lat-lat) + abs(prev_lat-lat)
            lon_diff = abs(next_lon - lon) + abs(prev_lon-lon)
            variance=lat_diff + lon_diff        # number tells how close this vertex is with the nighbours
                                # used decending iteration bc edit affects index values
            #print("numb: "+ str(scanspot) + " - " +str(variance))
            if variance < tollerance:
                coords.pop(scanspot)        # deletes this specific coordinate
                affected_coord +=1
            else:
                geom_text += coords[scanspot] + ","   
        scanspot -= 1 
    
    geom_text += geom_last + ")))"        # back to original format
    print("numb: "+ str(scanspot)+" Coords: " + str(coodlength) + "  Affected: " + str(affected_coord))
    return geom_text
    

def simplify_class(csv_path, output_path):
    #csv file name:
    airspace_csv = "Airspace_q_csv.csv"
    tolerance_value = 0.005
    # convert csv to temporary json - 
    csv_file = pd.DataFrame(pd.read_csv(csv_path, sep=',', header = 0, index_col = False))
    csv_file.to_json("temp_class_Airspace_JSON.json", orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)

    # Open and read the JSON file
    with open('temp_class_Airspace_JSON.json', 'r') as file:
        data = json.load(file)
    

    # the coordinate information should be converted to array, for mathematical analysis
    # Specify the field key to update
    field_key = 'geometry'

    # Update the specified field value
    array_numb = 0
    for item in data:
        if field_key in data[array_numb]:
            data[array_numb][field_key] = geometry_array(array_numb, data, field_key) # geometry_array function is called
        array_numb += 1

    # the above edit is written to new json file   
    with open('temp_converted.json', 'w') as f:
        json.dump(data, f)

        
    # Open and read the the temporary JSON file
    with open('temp_converted.json', 'r') as file:
        newdata = json.load(file)

    if False:
        #print(newdata[0]['WKT'])
        #print(newdata[1]['WKT'])
        coords = newdata[0]['WKT']
        prev_lat=coords[0].split()[0]
        print(str(prev_lat))


    # run through the data to simplify the coordinate and assign new value
    iter=0
    for newitem in newdata:
        coords = newdata[iter][field_key][10:]
        if coords:
            newdata[iter][field_key] = coord_variation(coords, tolerance_value)  # function called, with tolerance
        iter += 1 

    # create a new temporary json file to save the new edit
    print("creating optimized json")
    with open('temp_optimized.json', 'w') as f:   
        json.dump(newdata, f)
        

    # convert the json back to csv
    csv_file_name = os.path.join(output_path, airspace_csv[0:-4] + "_optimized.csv")
    with open('temp_optimized.json', encoding='utf-8-sig') as f_input:
        df = pd.read_json(f_input)
    df.to_csv(csv_file_name, encoding='utf-8', index=False)

    # delete intermidiate files
    if os.path.exists("temp_class_Airspace_JSON.json"):
        os.remove("temp_class_Airspace_JSON.json")
    if os.path.exists("temp_converted.json"):
        os.remove("temp_converted.json")
    if os.path.exists("temp_optimized.json"):
        os.remove("temp_optimized.json")


def create_sua(url, geometry=False):
    print('creating csv file ...')
    for _ in tqdm(range(1)):
        while True:
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status() 
                break
            except Exception as e:
                #print(e)
                time.sleep(10)

        data = json.loads(response.text)['features']
        pdata = []
        if not geometry:
            for d in data:
                pdata.append(d['attributes'])
        else:
            for d in data:
                pdata.append({**d['geometry'], **d['attributes']})

        file_path = os.path.join(download_dir, 'edata.json')
        f = open(file_path, 'w')
        json.dump(pdata, f, default=str)
        f.close()

    return file_path


def download_tfr_files(url, output_path):
    links = list(set(get_links(url, 'save_pages/detail_')))
    print('downloading TFR files ...')
    flag = False
    files_list = []
    dates_list = []
    for link in tqdm(links):
        digits = re.findall(r'\d+', link)
        shp_link = url[:20] + link[3:14] + digits[0] + '_' + digits[1] + '.shp.zip'
        page_link = url[:20] + link[3:14] + 'detail_' + digits[0] + '_' + digits[1] + '.html'
        while True:
            try:
                file_name = os.path.join(output_path, shp_link[(shp_link.rfind('/')+1):])
                urllib.request.urlretrieve(shp_link, file_name)
                files_list.append(file_name)
                #break
            except Exception as e:
                if e.code == 404:
                    flag = True
                    break
                time.sleep(10)
                continue
            else:
                while True:
                    try:
                        response = requests.get(page_link, headers=headers)
                        response.raise_for_status()
                        soup = BeautifulSoup(response.text, 'html.parser')
                        #dates = [font.get_text(strip=True) for font in soup.find_all('font') 
                        #         if ('From ' in font.text or 'To ' in font.text or 'to ' in font.text) and 'UTC' in font.text]
                        dates = []
                        for tr in soup.find_all('tr'):
                            cells = tr.find_all('font')
                            if len(cells) > 1:
                                label = cells[0].get_text(strip=True)
                                if "Beginning Date and Time" in label:
                                    dates.append(cells[1].get_text(strip=True))
                                elif "Ending Date and Time" in label:
                                    dates.append(cells[1].get_text(strip=True))
                        date_time = ''
                        count = 0
                        for d in dates:
                            count += 1
                            if count > 2:
                                break
                            if count == 1:
                                date_time += 'From ' + d
                            else:
                                date_time += ' to ' + d
                        dates_list.append(date_time)
                        break
                    except:
                        time.sleep(10)
                        continue
                break
                
        if flag:
            flag = False
            continue
    
    return [files_list, dates_list]


def download_offline_maps(url, output_path):
    links = list(set(get_links(url, '/PDFs/')))
    print('downloading Offline maps ...')

    for link in tqdm(links):
        while True:
            try:
                urllib.request.urlretrieve(link, os.path.join(output_path, link[(link.rfind('/')+1):]))
                break
            except:
                time.sleep(10)
                continue           
        

def invert_image(img_path, output_path):
    # Open the image
    image = Image.open(img_path)
    
    # Convert the image to RGBA if it isn't already
    if image.mode != 'RGBA':
        image = image.convert('RGBA')
    
    # Convert to numpy array for easier manipulation
    img_array = np.array(image)
    
    # Separate the alpha channel
    alpha = img_array[:, :, 3]
    
    # Invert only the RGB channels (leave alpha unchanged)
    img_array[:, :, :3] = 255 - img_array[:, :, :3]
    
    # Restore the original alpha channel
    img_array[:, :, 3] = alpha
    
    # Convert back to PIL Image
    inverted_image = Image.fromarray(img_array)
    
    # Save the inverted image
    inverted_image.save(output_path)
    return inverted_image


def download_airport_diagrams(urls, output_path):
    print('downloading Airport diagrams ...')
    for airport_id in tqdm(urls):
        id = airport_id[(airport_id.rfind('/')+1):]
        flag = False
        while True:
            try:
                img_url = "https://www.aopa.org/ustprocs/airportgraphics/gif/tn_" + id + "_tif.gif"
                response = requests.get(img_url, headers=headers)
                response.raise_for_status()
                flag = True
                break
            except:
                img_url = "https://www.aopa.org/ustprocs/airportgraphics/gif/tn_" + id[1:] + "_tif.gif"
                try:
                    response = requests.get(img_url)
                    response.raise_for_status()
                    flag = True
                    break
                except:
                    break
        if flag:
            while True:
                try:
                    img_data = response.content
                    img_name = os.path.join(output_path, airport_id[(airport_id.rfind('/')+1):] + '_diagram.png')
                    with open(img_name, 'wb') as handler:
                        handler.write(img_data)
                    
                    invert_image(img_name, img_name.replace('.png', '_invert.png'))
                                 
                    break
                except Exception as e:
                    break


def get_link_to_current_data(url, file):
    current = get_links(url, '/NASR_Subscription/')
    #data = get_links(url + current[-1][-10:], file)
    #return data[0]
    return 'https://nfdc.faa.gov/webContent/28DaySub/28DaySubscription_Effective_' + current[-1][(current[-1].rfind('/')+1):] + '.zip'


def get_name_of_csv_zip(zip_name):
    #16_May_2024_CSV.zip
    day = zip_name[-6:-4]
    month = zip_name[-9:-7]
    year = zip_name[-14:-10]
    return day + '_' + str(calendar.month_name[int(month)])[:3] + '_' + year + '_CSV.zip'


def delete_previous_content(arg):
    content_dirs = os.listdir(output_dir)
    for dir in content_dirs:
        if len(dir) > 10 and dir[10] == arg:
            shutil.rmtree(os.path.join(output_dir, dir))
            break

def download_28dayNASR_zip(url):
    flag = 2
    y = datetime.now().year
    m = datetime.now().month
    d = datetime.now().day
    while True:
        try:
            file_name_of_28dayNASR_zip = download_file_from_url(
                url + str(y) + '-' + str(m).zfill(2) + '-' + str(d).zfill(2) + '.zip',
                download_dir
                )
            break
        except:
            d -= 1
            if d <= 0:
                d = 31
                m -= 1
                flag -= 1
                if not(flag):
                    break
                if m <= 0:
                    m = 12
                    y -= 1
    if not(flag):
        file_name_of_28dayNASR_zip = download_file_from_url(
                get_link_to_current_data(data_link, '/28DaySub/28DaySubscription_Effective_'),
                download_dir
                )
    return file_name_of_28dayNASR_zip


def extract_tpa_rgt():
    pass

def check_all_args(a, b, f, o, d, w, t, c, e, g, m, n, r):
    return a or b or f or o or d or w or t or c or e or g or m or n or r

def main():
         
    parser = argparse.ArgumentParser(description="Process some arguments.")
    parser.add_argument('-a', action='store_true')
    parser.add_argument('-b', action='store_true')
    parser.add_argument('-f', action='store_true')
    parser.add_argument('-o', action='store_true')
    parser.add_argument('-d', action='store_true')
    parser.add_argument('-w', action='store_true')
    parser.add_argument('-t', action='store_true')
    parser.add_argument('-s', action='store_true')
    parser.add_argument('-c', action='store_true')
    parser.add_argument('-e', action='store_true')
    parser.add_argument('-g', action='store_true')
    parser.add_argument('-m', action='store_true')
    parser.add_argument('-n', action='store_true')
    parser.add_argument('-r', action='store_true')
    

    args = parser.parse_args()

    if args.b or args.f or args.w or args.c or args.n or args.r or args.a or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step 0
        print('\nstep 0 (downloading 28dayNASR_zip) start ', datetime.now())
        #file_name_of_28dayNASR_zip = download_28dayNASR_zip('https://nfdc.faa.gov/webContent/28DaySub/28DaySubscription_Effective_')
        #file_name_of_28dayNASR_zip = "downloaded_data/28DaySubscription_Effective_2024-10-31.zip"
        #"""
        file_name_of_28dayNASR_zip = download_file_from_url(
                get_link_to_current_data(data_link, '/28DaySub/28DaySubscription_Effective_'),
                download_dir
                )#"""
        path_to_file_to_extract = 'CSV_Data/' + get_name_of_csv_zip(file_name_of_28dayNASR_zip)
        print('step 0 (downloading 28dayNASR_zip) finish ', datetime.now())
    
    if args.b or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -b
        arg = 'b'
        print('step (-', arg, ') start ', datetime.now())
        file_to_extract = 'APT_BASE.csv'
        file_name_of_extracted_file1 = extract_file_from_nested_zip(file_name_of_28dayNASR_zip,
                                                                    path_to_file_to_extract,
                                                                    file_to_extract,
                                                                    download_dir)
        file_to_extract = 'FRQ.csv'
        file_name_of_extracted_file2 = extract_file_from_nested_zip(file_name_of_28dayNASR_zip,
                                                                    path_to_file_to_extract,
                                                                    file_to_extract,
                                                                    download_dir)
        file_to_extract = 'APT_RWY.csv'
        file_name_of_extracted_file3 = extract_file_from_nested_zip(file_name_of_28dayNASR_zip,
                                                                    path_to_file_to_extract,
                                                                    file_to_extract,
                                                                    download_dir)
        file_to_extract = 'AWOS.csv'
        file_name_of_extracted_file4 = extract_file_from_nested_zip(file_name_of_28dayNASR_zip,
                                                                    path_to_file_to_extract,
                                                                    file_to_extract,
                                                                    download_dir)

        #file_name_of_cs_all = 'downloaded_data/CS_ALL_20241031.zip'
        #"""
        file_name_of_cs_all = download_file_from_url(
            get_links('https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/dafd/', '/CS_ALL_')[0],
            download_dir
            )#"""
        cs_pdfs = extract_all_files(file_name_of_cs_all, download_dir)

        dir = create_content_dir(arg)
        file_name = 'base_file'
        file_path = os.path.join(dir, file_name + '_' + get_today_date_str(d=False) + '.csv')
        airport_urls = create_base_file(
                            file_name_of_extracted_file1,
                            file_name_of_extracted_file2,
                            file_name_of_extracted_file3,
                            file_name_of_extracted_file4,
                            cs_pdfs,
                            file_path,
                            base_airpot_url)
        update_table_from_csv(file_name, file_path)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
    
    if args.f or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -f
        arg = 'f'
        print('step (-', arg, ') start ', datetime.now())
        file_to_extract = 'FIX_BASE.csv'
        file_name_of_extracted_file = extract_file_from_nested_zip(file_name_of_28dayNASR_zip,
                                                                    path_to_file_to_extract,
                                                                    file_to_extract,
                                                                    download_dir)

        dir = create_content_dir(arg)
        file_name = 'waypoint_file'
        file_path = os.path.join(dir, file_name + '_' + get_today_date_str(d=False) + '.csv')
        create_waypoint_file(
            file_name_of_extracted_file,
            file_path
            )
        update_table_from_csv(file_name, file_path)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
  
    if args.o or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -o
        arg = 'o'
        print('step (-', arg, ') start ', datetime.now())
        DOF_url = get_links("https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/dof/", '.zip')
        file_name_of_DOF_DAT_zip = download_file_from_url(DOF_url[-1],
                                                                download_dir)

        #print('extracting  ' + file_name_of_DOF_DAT_zip + ' ...')  
        dir = file_name_of_DOF_DAT_zip[:-4]
        os.makedirs(dir, exist_ok=True)
        file_name_of_extracted_file = extract_all_files(file_name_of_DOF_DAT_zip,
                                                       dir)
    
        dof_dir = create_content_dir(arg)
        input_files = os.listdir(dir)
        file_name = 'obstacle_file'
        file_path = os.path.join(dir, file_name + '_' + get_today_date_str(d=False) + '.csv')
        create_obstacle_file(
            input_files,
            dir,
            file_path
            )
        update_table_from_csv(file_name, file_path)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
    
    if args.d or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -d
        arg = 'd'
        print('step (-', arg, ') start ', datetime.now())
        file_name_of_DAILY_DOF_DAT_zip = download_file_from_url("https://aeronav.faa.gov/Obst_Data/DAILY_DOF_CSV.ZIP",
                                                                download_dir)
        DOF_file = 'DOF.csv'
        #print('extracting  ' + DOF_file + ' ...')  
        file_name_of_extracted_file = extract_single_file(file_name_of_DAILY_DOF_DAT_zip,
                                                            DOF_file,
                                                            download_dir)
    
        dir = create_content_dir(arg)
        file_name = 'daily_obstacle_file'
        file_path = os.path.join(dir, file_name + '_' + get_today_date_str(d=False) + '.csv')
        create_daily_obstacle_file(
            file_name_of_extracted_file,
            file_path
        )
        update_table_from_csv(file_name, file_path)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
    
    if args.w or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -w
        arg = 'w'
        print('step (-', arg, ') start ', datetime.now())
        delete_previous_content(arg)
        dir = create_content_dir(arg)
        file_name = 'wx_file'
        file_path = os.path.join(dir, file_name + '_' + get_today_date_str(d=False) + '.csv')    
        file_to_extract = 'APT_BASE.csv'
        file_name_of_extracted_file = extract_file_from_nested_zip(file_name_of_28dayNASR_zip,
                                                                    path_to_file_to_extract,
                                                                    file_to_extract,
                                                                    download_dir)
        create_wx_file(
            "https://aviationweather.gov/data/cache/metars.cache.csv.gz",
            file_path,
            file_name_of_extracted_file
            )
        update_table_from_csv(file_name, file_path)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
    
    if args.t or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -t
        arg = 't'
        print('step (-', arg, ') start ', datetime.now())
        delete_previous_content(arg)
        files_list = download_tfr_files(
            "https://tfr.faa.gov/tfr2/list.html",
            download_dir
            )
        dir = create_content_dir(arg)
        convert_shp_to_csv(
            files_list,
            download_dir,
            dir
        )
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
        
    if args.c or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -c
        arg = 'c'
        print('step (-', arg, ') start ', datetime.now())
        dir = create_content_dir(arg)

        shape_file_path = 'Additional_Data/Shape_Files/Class_Airspace.shp'
        extract_single_file(file_name_of_28dayNASR_zip, shape_file_path.replace('.shp', '.dbf'), download_dir)
        extract_single_file(file_name_of_28dayNASR_zip, shape_file_path.replace('.shp', '.prj'), download_dir)
        extract_single_file(file_name_of_28dayNASR_zip, shape_file_path, download_dir)
        extract_single_file(file_name_of_28dayNASR_zip, shape_file_path.replace('.shp', '.shx'), download_dir)
        
        file_path = create_class_airspace(shape_file_path, dir)
        file_name = 'Class_Airspace'
                       
        update_table_from_csv(file_name.replace, file_path)     
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
        
    if args.e or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -e
        arg = 'e'
        print('step (-', arg, ') start ', datetime.now())
        dir = create_content_dir(arg)

        url = 'https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/Special_Use_Airspace/FeatureServer/0/query?where=1%3D1&outFields=NAME,TYPE_CODE,UPPER_VAL,UPPER_UOM,LOWER_VAL,LOWER_UOM,CITY,TIMESOFUSE&outSR=4326&f=json'

        file_path = create_sua(url)

        file_name = 'sua'
        file = os.path.join(dir,  file_name + '_' + get_today_date_str(d=False) + '.csv')
        df = pd.read_json(file_path)  
        df.to_csv(file, index=False) 
                            
        update_table_from_csv(file_name, file_path)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
        
    if args.g or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -g
        arg = 'g'
        print('step (-', arg, ') start ', datetime.now())
        dir = create_content_dir(arg)

        url = 'https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/Stadiums/FeatureServer/0/query?where=1%3D1&outFields=NAME,CITY,STATE&outSR=4326&f=json'

        file_path = create_sua(url, True)
                            
        file_name = 'stadium'
        file = os.path.join(dir,  file_name + '_' + get_today_date_str(d=False) + '.csv')
        df = pd.read_json(file_path)  
        df.to_csv(file, index=False) 

        update_table_from_csv(file_name, file_path)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
    
    if args.a or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -a
        arg = 'a'
        print('step (-', arg, ') start ', datetime.now())
        dir = create_content_dir(arg)
        if not(args.b):
            file_to_extract = 'APT_BASE.csv'
            file_name_of_extracted_file = extract_file_from_nested_zip(file_name_of_28dayNASR_zip,
                                                                            path_to_file_to_extract,
                                                                            file_to_extract,
                                                                            download_dir)
            airport_urls = get_airport_urls(base_airpot_url, file_name_of_extracted_file)
        download_airport_diagrams(airport_urls,
                                 dir)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)

    if args.m or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -m
        arg = 'm'
        print('step (-', arg, ') start ', datetime.now())
        dir = create_content_dir(arg)
        download_offline_maps("https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/vfr/",
                            dir)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)

    if args.n or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -n
        arg = 'n'
        print('step (-', arg, ') start ', datetime.now())
        file_to_extract = 'NAV_BASE.csv'
        file_name_of_extracted_file = extract_file_from_nested_zip(file_name_of_28dayNASR_zip,
                                                                    path_to_file_to_extract,
                                                                    file_to_extract,
                                                                    download_dir)

        dir = create_content_dir(arg)
        file_name = 'nav'
        file_path = os.path.join(dir, file_name + '_' + get_today_date_str(d=False) + '.csv')
        create_nav_file(
            file_name_of_extracted_file,
            file_path
            )
        update_table_from_csv(file_name, file_path)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
        
    if args.r or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.c, args.e, args.g, args.m, args.n, args.r)):
        # step -r
        arg = 'r'
        print('step (-', arg, ') start ', datetime.now())
        file_to_extract = 'APT_RWY_END.csv'
        file_name_of_extracted_file = extract_file_from_nested_zip(file_name_of_28dayNASR_zip,
                                                                    path_to_file_to_extract,
                                                                    file_to_extract,
                                                                    download_dir)
        dir = create_content_dir(arg)
        file_name = 'RWY_END'
        file_path = os.path.join(dir, file_name + '_' + get_today_date_str(d=False) + '.csv')
        create_rwy_end(
            file_name_of_extracted_file,
            file_path
            )
        update_table_from_csv(file_name, file_path)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)


    return

if __name__ == "__main__":
    main()

