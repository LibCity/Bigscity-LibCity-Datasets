import requests
import zipfile
import tempfile
 
 
def get_data(url):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        print(f"URL:{url} is not valid")
    return url, response.content
 
 
if __name__ == '__main__':
    # 2019/1-2022/1
    for year in ["2019","2020","2021","2022"]:
        for month in [str(i).zfill(2) for i in range(1,13)]:
            if int(year) >=2022 and int(month) >1:
                continue 
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
