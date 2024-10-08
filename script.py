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
                    urllib.request.urlretrieve(url, filename)
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

def create_wx_file(url, output_file):
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


def create_base_file(input_file, input_file2, input_file3, output_file, base_url):
    print('creating  base file.csv ...')
    output_fields = ['Identifier','City','State','Country','Lat','Long','Elevation','CTAF','UNICOM','ATIS',
                     'AWOS','ASOS','GROUND','TOWER', 'TPA']
    
    output_rows = []

    airport_urls = []

    max_runways = 1

    with open(input_file, 'r', encoding="utf-8") as csv_file:
        f = csv.reader(csv_file)
        #skip_first = True
        row_count = sum(1 for row in f)
        csv_file.seek(0)
        with alive_bar(row_count) as bar:
            for row in f:
                airport_url = base_url + row[4]
                airport_urls.append(airport_url)
                last_fields = get_last_fields(input_file2, row[4])
                #last_fields = [0,0,0,0,0,0,0]              

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
                        else:
                            if flag:
                                break
                    if count_runways > max_runways:
                        max_runways = count_runways

                output_row = [row[4], row[5], row[3], row[6], row[19], row[24], row[26],
                                    last_fields[0], last_fields[1], last_fields[2], last_fields[3],
                                   last_fields[4], last_fields[5], last_fields[6]]
                output_row.extend(runways)

                output_rows.append(copy.copy(output_row))
                #if max_runways == 12: break
                bar()

    for i in range(max_runways):
        output_fields.append('RWY_ID')
        output_fields.append('RWY_LEN')
        output_fields.append('RWY_WIDTH')
        
    del output_rows[0]

    with open(output_file, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(output_fields)
        csvwriter.writerows(output_rows)

    return airport_urls



def get_last_fields(input_file, arpt_id):      
    with open(input_file, 'r', encoding="utf-8") as csv_file3:
        h = csv.reader(csv_file3)
        values = [''] * 7
        flag = False
        for row in h:
            if row[1] == arpt_id:
                flag = True
                if row[19].find('CTAF') != -1:
                    values[0] = row[17]
                elif row[19].find('UNICOM') != -1:
                    values[1] = row[17]
                elif row[19].find('ATIS') != -1:
                    values[2] = row[17]
                elif row[19].find('AWOS') != -1:
                    values[3] = row[17]
                elif row[19].find('ASOS') != -1:
                    values[4] = row[17]
                elif row[19].find('GND/P') != -1:
                    values[5] = row[17]
                elif row[19].find('TOWER') != -1:
                    values[6] = row[17]
            else:
                if flag:
                    break

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
    output_fields = ['ARPT_ID','RWY_ID','LAT_DECIMAL','LONG_DECIMAL']
    
    output_rows = []

    with open(input_file, 'r', encoding="utf-8") as csv_file:
        f = csv.reader(csv_file)
        row_count = sum(1 for row in f)
        csv_file.seek(0)
        with alive_bar(row_count) as bar:
            for row in f:
                output_rows.append([row[4], row[7], row[18], row[23]])
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

def extract_all_files(path, output_path=''):
    print('extracting  ' + path + ' ...') 
    for i in tqdm(range(1)):
        with zipfile.ZipFile(path, 'r') as zip:
            zip.extractall(output_path)
    return zip.namelist()


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
                    break
                except:
                    break


def get_link_to_current_data(url, file):
    current = get_links(url, '/NASR_Subscription/')
    data = get_links(url + current[-1][-10:], file)
    return data[0]


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


def check_all_args(a, b, f, o, d, w, t, s, m, n, r):
    return a or b or f or o or d or w or t or s or m or n or r


def extract_tpa_rgt():
    pass


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
    parser.add_argument('-m', action='store_true')
    parser.add_argument('-n', action='store_true')
    parser.add_argument('-r', action='store_true')
    

    args = parser.parse_args()

     
    if args.b or args.f or args.n or args.r or args.a or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.s, args.m, args.n, args.r)):
        # step 0
        #print('downloading 28dayNASR_zip ...')
        print('step 0 (downloading 28dayNASR_zip) start ', datetime.now())
        file_name_of_28dayNASR_zip = download_file_from_url(
            get_link_to_current_data(data_link, '/28DaySub/28DaySubscription_Effective_'),
            download_dir
            )
        path_to_file_to_extract = 'CSV_Data/' + get_name_of_csv_zip(file_name_of_28dayNASR_zip)
        print('step 0 (downloading 28dayNASR_zip) finish ', datetime.now())
    
    if args.b or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.s, args.m, args.n, args.r)):
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

        file_name_of_cs_all = download_file_from_url(
            get_links('https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/dafd/', '/CS_ALL_')[0],
            download_dir
            )
        cs_pdfs = extract_all_files(file_name_of_cs_all, download_dir)


        dir = create_content_dir(arg)
        airport_urls = create_base_file(
                            file_name_of_extracted_file1,
                            file_name_of_extracted_file2,
                            file_name_of_extracted_file3,
                            os.path.join(dir, 'base_file_' + get_today_date_str(d=False) + '.csv'),
                            base_airpot_url)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
    
    if args.f or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.s, args.m, args.n, args.r)):
        # step -f
        arg = 'f'
        print('step (-', arg, ') start ', datetime.now())
        file_to_extract = 'FIX_BASE.csv'
        file_name_of_extracted_file = extract_file_from_nested_zip(file_name_of_28dayNASR_zip,
                                                                    path_to_file_to_extract,
                                                                    file_to_extract,
                                                                    download_dir)

        dir = create_content_dir(arg)
        create_waypoint_file(
            file_name_of_extracted_file,
            os.path.join(dir, 'waypoint_file_' + get_today_date_str(d=False) + '.csv')
            )
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
  
    if args.o or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.s, args.m, args.n, args.r)):
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
        create_obstacle_file(
            input_files,
            dir,
            os.path.join(dof_dir, 'obstacle_file_' + get_today_date_str(d=False) + '.csv')
            )
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
    
    if args.d or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.s, args.m, args.n, args.r)):
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
        create_daily_obstacle_file(
            file_name_of_extracted_file,
            os.path.join(dir, 'daily_obstacle_file_' + get_today_date_str(d=False) + '.csv')
        )
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
    
    if args.w or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.s, args.m, args.n, args.r)):
        # step -w
        arg = 'w'
        print('step (-', arg, ') start ', datetime.now())
        delete_previous_content(arg)
        dir = create_content_dir(arg)
        create_wx_file(
            "https://aviationweather.gov/data/cache/metars.cache.csv.gz",
            os.path.join(dir, 'wx_file_' + get_today_date_str(d=False) + '.csv')
                        )
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
    
    if args.t or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.s, args.m, args.n, args.r)):
        # step -t
        arg = 't'
        print('step (-', arg, ') start ', datetime.now())
        delete_previous_content(arg)
        dir = create_content_dir(arg)
        download_tfr_files("https://tfr.faa.gov/tfr2/list.html",
                           dir)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
        
    if args.s or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.s, args.m, args.n, args.r)):
        # step -s
        arg = 's'
        print('step (-', arg, ') start ', datetime.now())
        dir = create_content_dir(arg)
        file_name_of_SHAPE_FILES_ZIP = download_file_from_url(
            get_link_to_current_data(data_link, '/class_airspace_shape_files.zip'),
            dir)
        #print('extracting SHAPE_FILES_ZIP ...')
        #os.makedirs(shape_dir, exist_ok=True)
        #extract_all_files(file_name_of_SHAPE_FILES_ZIP, shape_dir)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
    
    if args.a or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.s, args.m, args.n, args.r)):
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

    if args.m or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.s, args.m, args.n, args.r)):
        # step -m
        arg = 'm'
        print('step (-', arg, ') start ', datetime.now())
        dir = create_content_dir(arg)
        download_offline_maps("https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/vfr/",
                            dir)
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)

    if args.n or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.s, args.m, args.n, args.r)):
        # step -n
        arg = 'n'
        print('step (-', arg, ') start ', datetime.now())
        file_to_extract = 'NAV_BASE.csv'
        file_name_of_extracted_file = extract_file_from_nested_zip(file_name_of_28dayNASR_zip,
                                                                    path_to_file_to_extract,
                                                                    file_to_extract,
                                                                    download_dir)

        dir = create_content_dir(arg)
        create_nav_file(
            file_name_of_extracted_file,
            os.path.join(dir, 'nav_' + get_today_date_str(d=False) + '.csv')
            )
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)
        
    if args.r or not(check_all_args(args.a, args.b, args.f, args.o, args.d, args.w, args.t, args.s, args.m, args.n, args.r)):
        # step -r
        arg = 'r'
        print('step (-', arg, ') start ', datetime.now())
        file_to_extract = 'APT_RWY_END.csv'
        file_name_of_extracted_file = extract_file_from_nested_zip(file_name_of_28dayNASR_zip,
                                                                    path_to_file_to_extract,
                                                                    file_to_extract,
                                                                    download_dir)
        dir = create_content_dir(arg)
        create_rwy_end(
            file_name_of_extracted_file,
            os.path.join(dir, 'RWY_END_' + get_today_date_str(d=False) + '.csv')
            )
        print('step (-', arg, ') finish ', datetime.now())
        commit_changes(arg)


    return

if __name__ == "__main__":
    main()

