from typing import Union
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
import os
import datetime
import platform
import csv
import json

app = FastAPI()

if platform.system() == "Windows":
    output_dir = 'home/'
else:
    output_dir = '/home/mb/processed_data/'


def get_latest_dir(arg, previous=True):
    list_dir = os.listdir(output_dir)
    #print('list_dir = ', list_dir)
    dir_dates = []
    for dir in list_dir:
        if dir.find('-content') == -1:
            dir_dates.append(datetime.datetime(1, 1, 1))
            continue
        if dir[10] == arg:
            ddl =  [s for s in dir if s.isdigit()]
            dir_date = datetime.datetime(
                int(ddl[0]+ddl[1]+ddl[2]+ddl[3]),
                int(ddl[4]+ddl[5]),
                int(ddl[6]+ddl[7]),
                )
            dir_dates.append(dir_date)
        else:
            dir_dates.append(datetime.datetime(1, 1, 1))
    #print('dir_dates == ', dir_dates)
    latest_dir = list_dir[dir_dates.index(max(dir_dates))]
    path_to_latest_dir = os.path.join(output_dir, latest_dir)
    while not(os.listdir(path_to_latest_dir)) and len(list_dir) > 1:
        del list_dir[dir_dates.index(max(dir_dates))]
        del dir_dates[dir_dates.index(max(dir_dates))]
        latest_dir = list_dir[dir_dates.index(max(dir_dates))]
        path_to_latest_dir = os.path.join(output_dir, latest_dir)

    return path_to_latest_dir

def get_latest_file(arg):
    latest_dir = get_latest_dir(arg)

    list_files = os.listdir(latest_dir)
    if not(list_files):
        return "This file doesn't exist"
    #print('list_files = ', list_files)
    file_times = []
    for f in list_files:
        ddl =  [s for s in f if s.isdigit()]
        f_time = datetime.time(
            int(ddl[0]+ddl[1]),
            int(ddl[2]+ddl[3]),
            int(ddl[4]+ddl[5]),
            )
        file_times.append(f_time)
    latest_file = list_files[file_times.index(max(file_times))]
            
    return os.path.join(latest_dir, latest_file)

def csv_to_json(csvFilePath, arr=False):
    data = []
    with open(csvFilePath, encoding='unicode_escape') as csvf:
        if arr:
            csvReader = csv.reader(csvf)
        else:
            csvReader = csv.DictReader(csvf)
        for row in csvReader:
            data.append(row)
    return data


@app.get("/changes")
def get_t(file_name='changes.json'):
    f = open(file_name)
    changes = json.load(f)
    f.close()
    return JSONResponse(content=changes)


@app.get("/b")
def get_t(arg='b'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    return JSONResponse(content=list_files)

@app.get("/b/base_file")
def get_b(arg='b'):
    return JSONResponse(csv_to_json(get_latest_file(arg)))


@app.get("/f")
def get_t(arg='f'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    return JSONResponse(content=list_files)

@app.get("/f/waypoint_file")
def get_f(arg='f'):
    return JSONResponse(csv_to_json(get_latest_file(arg)))


@app.get("/o")
def get_t(arg='o'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    return JSONResponse(content=list_files)

@app.get("/o/obstacle_file")
def get_o(arg='o'):
    return JSONResponse(csv_to_json(get_latest_file(arg)))


@app.get("/d")
def get_t(arg='d'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    return JSONResponse(content=list_files)

@app.get("/d/daily_obstacle_file")
def get_d(arg='d'):
    #return FileResponse(get_latest_file(arg))
    return JSONResponse(csv_to_json(get_latest_file(arg)))


@app.get("/w")
def get_t(arg='w'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    return JSONResponse(content=list_files)

@app.get("/w/wx_file")
def get_w(arg='w'):
    return JSONResponse(csv_to_json(get_latest_file(arg)))


@app.get("/t")
def get_t(arg='t'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    return JSONResponse(content=list_files)


@app.get("/t/{file_name}")
def get_t(file_name: str, arg='t'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    #print(list_dir)
    if file_name in list_files:
        return FileResponse(os.path.join(list_dir, file_name))
    else:
        return 'This file does not exist'


@app.get("/s")
def get_t(arg='s'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    return JSONResponse(content=list_files)


@app.get("/s/shape_files.zip")
def get_s(arg='s'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    return FileResponse(list_files[0])


@app.get("/a")
def get_t(arg='a'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    return JSONResponse(content=list_files)

@app.get("/a/{file_name}")
def get_a(file_name: str, arg='a'):
    list_dir = get_latest_dir(arg)
    #print('LIST_dir === ', list_dir)
    list_files = os.listdir(list_dir)
    #print('list_files ===== ', list_files)
    if file_name in list_files:
        return FileResponse(os.path.join(list_dir, file_name))
    else:
        return 'This file does not exist'


@app.get("/m")
def get_t(arg='m'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    return JSONResponse(content=list_files)

@app.get("/m/{file_name}")
def get_m(file_name: str, arg='m'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    #print(list_dir)
    if file_name in list_files:
        return FileResponse(os.path.join(list_dir, file_name))
    else:
        return 'This file does not exist'
    

@app.get("/n")
def get_t(arg='n'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    return JSONResponse(content=list_files)

@app.get("/n/nav")
def get_b(arg='n'):
    return JSONResponse(csv_to_json(get_latest_file(arg)))


@app.get("/r")
def get_t(arg='r'):
    list_dir = get_latest_dir(arg)
    list_files = os.listdir(list_dir)
    return JSONResponse(content=list_files)

@app.get("/r/RWY_END")
def get_b(arg='r'):
    return JSONResponse(csv_to_json(get_latest_file(arg)))