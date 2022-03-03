import requests
import zipfile
import tempfile
import os
import pandas as pd
def get_data(url):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        print(f"URL:{url} is not valid")
    return url, response.content
 
def match_columns():
    """
    for data after 202102(including), the column has changed.
    This function map the new column back to original ones.
    
    """

    path,dir,files =  next(iter(os.walk("./")))
    col_set = set()   
    columns = {
        "ride_id":"bikeid",
        "rideable_type":"usertype",
        "started_at":"starttime",
        "ended_at":"stoptime",
        "start_station_name":"start station name",
        "start_station_id":"start station id",
        "end_station_name":"end station name",
        "end_station_id":"end station id",
        "start_lat":"start station latitude",
        "start_lng":"start station longitude",
        "end_lat":"end station latitude",
        "end_lng":"end station longitude",
        "member_casual":"gender",
    } 
    for file in sorted(files):
        if file.endswith(".csv"):
            print(file)
            trip = pd.read_csv(file)
            col = trip.columns
            
            col = ", ".join(list(col))
            print(col)
            print("*"*10)
            col_set.add(col)
            if len(col_set) > 1:
                trip.rename(columns=columns,inplace=True)
                trip.to_csv(file)
            
        
            
    
if __name__ == '__main__':
    # 2019/1-2022/1
    for year in ["2019","2020","2021","2022"]:
        for month in [str(i).zfill(2) for i in range(1,13)]:
            if int(year) >=2022 and int(month) >1:
                break
                
            date = year  + month
            url = f"https://s3.amazonaws.com/tripdata/{date}-citibike-tripdata.csv.zip"
            url, data = get_data(url)  # data为byte字节
        
            _tmp_file = tempfile.TemporaryFile()  # 创建临时文件
            print(_tmp_file)
        
            _tmp_file.write(data)  # byte字节数据写入临时文件
            # _tmp_file.seek(0)
        
            zf = zipfile.ZipFile(_tmp_file, mode='r')
            for names in zf.namelist():
                f = zf.extract(names, './')  # 解压到zip目录文件下
                print(f)
        
            zf.close()
    match_columns()