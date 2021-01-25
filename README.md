# TrafficDL 数据集处理脚本

用于转换原始的开源数据集成为TrafficDL所需要的**原子文件**

约定：

- 使用路径`input/dataset_name/`存放原始数据集。

- 使用路径`output/dataset_dir`存放转换后的原子文件。
- 脚本名（`dataset_script`）用小写，输出路径（`dataset_dir`）用大写，二者都跟数据集同名。
- 对于数据集有多个子数据集（如`NYC_TAXI`有黄色车子集，也有绿色车子集）的，可以用多个脚本，输出到多个路径，保证脚本名、输出路径名与它处理的**子**数据集同名即可。（例如`nyc_taxi_green.py`）
- 对于数据集有多种处理方式的（如`NYC_TAXI`可以做`od`类型，也可以做`state`类型）的，可以用多个脚本，输出到多个路径，在原本的名字后边加`_type`区分即可。（例如`nyc_taxi_green_od.py`）
- 原子文件的文件名是数据集名或子数据集名，不用加`_type`。（例如`NYC_TAXI_Green_od/NYC_TAXI_Green.dyna`）

## 数据集列表

### Foursquare

### Foursquare：NYC Restaurant Rich Dataset

### Foursquare：Global-scale Check-in Dataset

### Foursquare：User Profile Dataset

### Foursquare：Global-scale Check-in Dataset with User Social Networks

### Gowalla

### Brightkite

### GeoLife_GPS

### NYC_Bus

### NYC_Taxi

### NYC_Bike

### BikeDC

### BikeCHI

### AustinRide

### I_80

### T_Drive

### Porto

### TaxiBJ

### Uber

### METR_LA

### Los_loop(=METR_LA)

### SZ_Taxi

### Loop Seattle

### NYC Speed data

### HK

### Q_Traffic

### ENG_HW

### PEMS

### PeMSD3

### PeMSD4

### PEMSD7

### PeMSD8

### PEMSD7(M)

### PeMSD_SF

### PEMS_BAY

### BusCHI

### NYC Accident data

### Road network data (OpenStreetMap)

https://www.openstreetmap.org/

### Weather and events data

https://www.wunderground.com/

### External data

## 数据集统计信息

