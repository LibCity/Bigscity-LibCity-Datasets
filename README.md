# Dataset in LibCity

This repository is used to introduce the dataset in LibCity.

## Dataset Conversion Tools

The dataset used in LibCity is stored in a unified data storage format named [atomic files](https://bigscity-libcity-docs.readthedocs.io/en/latest/user_guide/data/atomic_files.html). In order to directly use [the datasets we collected](https://bigscity-libcity-docs.readthedocs.io/en/latest/user_guide/data/raw_data.html) in LibCity, we have converted all datasets into the format of atomic files, and provide the conversion tools in this repository.

All conversion tools take the original dataset in the `./input/` directory as input, and output the converted atomic files to the `./output/` directory. In addition, we provide a link to obtain the original dataset in the first line of each conversion tool. You can download the original dataset through this link and place it in the `./input/` directory. Imitating our conversion tools, you can easily convert your own traffic dataset to adapt it to LibCity.

Besides, you can simply download the datasets we have processed, the data link is [BaiduDisk with code 1231](https://pan.baidu.com/s/1qEfcXBO-QwZfiT0G3IYMpQ) or [Google Drive](https://drive.google.com/drive/folders/1g5v2Gq1tkOq8XO0HDCZ9nOTtRpB6-gPe?usp=sharing).

## Dataset Statistics Infomation

Here we present the statistics of the  datasets we have processed.

- [Traffic State Datasets: Point-based Flow/Speed/Occupancy](#Traffic-State-Datasets:-Point-based-Flow/Speed/Occupancy)
- [Traffic State Datasets: Grid-based In-Flow/Out-Flow](#Traffic-State-Datasets:-Grid-based-In-Flow/Out-Flow)
- [Traffic State Datasets: OD-based Flow](#Traffic-State-Datasets:-OD-based-Flow)
- [Traffic State Datasets: Grid-OD-based Flow](#Traffic-State-Datasets:-Grid-OD-based-Flow)
- [Traffic State Datasets: Risk](#Traffic-State-Datasets:-Risk)
- [GPS Point Trajectory Datasets](#GPS-Point-Trajectory-Datasets)
- [Road Segment-based Trajectory Datasets](#Road-Segment-based-Trajectory-Datasets)
- [POI-based Trajectory Datasets](#POI-based-Trajectory-Datasets)
- [Road Network Datasets](#Road-Network-Datasets)

### Traffic State Datasets: Point-based Flow/Speed/Occupancy

>Collected from sensors or Pre-processed from trajectory data.

| DATASET                   | #GEO   | #REL    | #USR | #DYNA       | PLACE                       | DURATION                         | INTERVAL |
| ------------------------- | ------ | ------- | ---- | ----------- | --------------------------- | -------------------------------- | -------- |
| METR_LA                   | 207    | 11,753  | —    | 7,094,304   | Los Angeles, USA            | Mar. 1, 2012 -   Jun. 27, 2012   | 5min     |
| LOS_LOOP                  | 207    | 42,849  | —    | 7,094,304   | Los Angeles, USA            | Mar. 1, 2012 -   Jun. 27, 2012   | 5min     |
| LOS_LOOP_SMALL            | 207    | 42,849  | —    | 417,312     | Los Angeles, USA            | May. 1, 2012 -   May. 5, 2012    | 5min     |
| SZ_TAXI                   | 156    | 24,336  | —    | 464,256     | Shenzhen, China             | Jan. 1, 2015 -   Jan. 31, 2015   | 15min    |
| LOOP_SEATTLE              | 323    | 104,329 | —    | 33,953,760  | Greater Seattle   Area, USA | over the entirely   of 2015      | 5min     |
| Q_TRAFFIC                 | 45,148 | 63,422  | —    | 264,386,688 | Beijing, China              | Apr. 1, 2017 - May 31, 2017      | 15min    |
| PEMSD3                    | 358    | 547     | —    | 9,382,464   | California, USA             | Sept. 1, 2018 -   Nov. 30, 2018  | 5min     |
| PEMSD4                    | 307    | 340     | —    | 5,216,544   | San Francisco Bay Area, USA | Jan. 1, 2018 -   Feb. 28, 2018   | 5min     |
| PEMSD7                    | 883    | 866     | —    | 24,921,792  | California, USA             | Jul. 1, 2016 -   Aug. 31, 2016   | 5min     |
| PEMSD8                    | 170    | 277     | —    | 3,035,520   | San Bernardino Area, USA    | Jul. 1, 2016 -   Aug. 31, 2016   | 5min     |
| PEMSD7(M)                 | 228    | 51,984  | —    | 2,889,216   | California, USA             | weekdays of May   and June, 2012 | 5min     |
| PEMS_BAY                  | 325    | 8,358   | —    | 16,937,700  | San Francisco Bay Area, USA | Jan. 1, 2017 -   Jun. 30, 2017   | 5min     |
| BEIJING_SUBWAY            | 276    | 76,176  | —    | 248,400     | Beijing, China              | Feb. 29, 2016 -   Apr. 3, 2016   | 30min    |
| M_DENSE                   | 30     | —       | —    | 525,600     | Madrid, Spain               | Jan. 1, 2018 -   Dec. 21, 2019   | 60min    |
| ROTTERDAM                 | 208    | —       | —    | 4,813,536   | Rotterdam, Holland          | 135 days of 2018                 | 2min     |
| SHMETRO                   | 288    | 82,944  | —    | 1,934,208   | Shanghai, China             | Jul. 1, 2016 -   Sept. 30, 2016  | 15min    |
| HZMETRO                   | 80     | 6,400   | —    | 146,000     | Hangzhou, China             | Jan. 1, 2019 - Jan. 25, 2019     | 15min    |
| NYCTAXI202001-202003_DYNA | 263    | 69,169  | —    | 574,392     | New York, USA               | Jan. 1, 2020 - Mar. 30, 2020     | 60min    |

### Traffic State Datasets: Grid-based In-Flow/Out-Flow

> Pre-processed from trajectory data.

| DATASET              | #GEO      | #REL      | #USR      | #DYNA       | PLACE                                                     | DURATION                         | INTERVAL |
| -------------------- | --------- | --------- | --------- | ----------- | --------------------------------------------------------- | -------------------------------- | -------- |
| TAXIBJ               | 32*32     | —         | —         | 5,652,480   | Beijing, China                                            | Mar. 1, 2015 -   Jun. 30, 2015 et al. | 30min    |
| T_DRIVE20150206      | 32*32     | 1,048,576 | —         | 3,686,400   | Beijing, China                                            | Feb. 1, 2015 -   Jun. 30, 2015   | 60min    |
| T_DRIVE_SMALL        | 32*32     | — | —         | 172,032 | Beijing, China                                            | Feb. 2, 2008 -   Feb. 8, 2008 | 60min    |
| NYCTAXI201401-201403_GRID | 10*20     | —         | —         | 432,000     | New York, USA                                             | Jan. 1, 2014 -   Mar. 31, 2014   | 60min    |
| NYCBIKE202007-202009 | 10*20     | —         | —         | 441,600     | New York, USA                                             | Jul. 1, 2020 -   Sept. 30, 2020  | 60min    |
| PORTO201307-201309 | 20*10     | —         | —         | 441,600     | Porto, Portugal                                           | Jul. 1, 2013 -   Sept. 30, 2013  | 60min    |
| AUSTINRIDE20160701-20160930 | 16*8      | —         | —         | 282,624     | Austin, USA                                               | Jul. 1, 2016 -   Sept. 30, 2016  | 60min    |
| BIKEDC202007-202009 | 16*8      | —         | —         | 282,624     | Washington, USA                                           | Jul. 1, 2020 -   Sept. 30, 2020  | 60min    |
| BIKECHI202007-202009 | 15*18     | —         | —         | 670,680     | Chicago, USA                                              | Jul. 1, 2020 -   Sept. 30, 2020  | 60min    |
| NYCTaxi20140112 | 15*5 | — | — | 1,314,000 | New York, USA | Jan. 1, 2014 -   Dec. 31, 2014 | 30min |
| NYCTaxi20150103 | 10*20 | — | — | 576,000 | New York, USA | Jan. 1, 2015 -   Mar. 1, 2015 | 30min |
| NYCTaxi20160102 | 16*12 | — | — | 552,960 | New York, USA | Jan. 1, 2016 -   Feb. 29, 2016 | 30min |
| NYCBike20140409 | 16*8 | — | — | 562,176 | New York, USA | Apr. 1, 2014 -   Sept. 30, 2014 | 60min |
| NYCBike20160708 | 10*20 | — | — | 576,000 | New York, USA | Jul. 1, 2016 -   Aug. 29, 2016 | 30min |
| NYCBike20160809 | 14*8 | — | — | 322,560 | New York, USA | Aug. 1, 2016 -   Sept. 29, 2016 | 30min |

### Traffic State Datasets: OD-based Flow

| DATASET                 | #GEO | #REL   | #USR | #DYNA       | PLACE         | DURATION                     | INTERVAL |
| ----------------------- | ---- | ------ | ---- | ----------- | ------------- | ---------------------------- | -------- |
| NYCTAXI202004-202006_OD | 263  | 69,169 | —    | 150,995,927 | New York, USA | Apr. 1, 2020 - Jun. 30, 2020 | 60min    |


### Traffic State Datasets: Grid-OD-based Flow

| DATASET              | #GEO      | #REL      | #USR      | #DYNA       | PLACE                                                     | DURATION                         | INTERVAL |
| -------------------- | --------- | --------- | --------- | ----------- | --------------------------------------------------------- | -------------------------------- | -------- |
| NYC_TOD              | 15\*5        | —     | —         | 98,550,000  | New York, USA | — |—|

### Traffic State Datasets: Risk

| DATASET              | #GEO      | #REL      | #USR      | #DYNA       | PLACE                                                     | DURATION                         | INTERVAL |
| -------------------- | --------- | --------- | --------- | ----------- | --------------------------------------------------------- | -------------------------------- | -------- |
| NYC_RISK             | 243       | 59049     | —         | 3504000     | New York, USA                                             | Jan. 01, 2013 - Dec. 31, 2013    | 60min    |
| CHICAGO_RISK         | 197       | 38809     | —         | 2332800     | Chicago, USA                                              | Feb. 01, 2016 - Sep. 30, 2016    | 60min    |


### GPS Point Trajectory Datasets

| DATASET              | #GEO      | #REL      | #USR      | #DYNA       | PLACE                                                     | DURATION                         | INTERVAL |
| -------------------- | --------- | --------- | --------- | ----------- | --------------------------------------------------------- | -------------------------------- | -------- |
| Chengdu_Taxi_Sample1 | —         | —         | 4565      | 712360      | Chengdu, China                                            | Aug. 03, 2014 - Aug. 30, 2014    | —        |
| Beijing_Taxi_Sample  | 16384        | —         | 76      | 518424      | Beijing, China                                            | Oct. 01, 2013 - Oct. 31, 2013    | —        |
| Seattle              | 613645    | 857406    | 1         | 7531        | Seattle WA, USA                                           | Jan.17,2009 20:27:37 - 22:34:28  | 1s       |
| Global               | 11045     | 18196     | 1         | 2502        | Neftekamsk, Republic of Bashkortostan, Russian Federation | —                                | 1s       |


### Road Segment-based Trajectory Datasets

| DATASET              | #GEO      | #REL      | #USR      | #DYNA       | PLACE                                                     | DURATION                         | INTERVAL |
| -------------------- | --------- | --------- | --------- | ----------- | --------------------------------------------------------- | -------------------------------- | -------- |


### POI-based Trajectory Datasets

| DATASET              | #GEO      | #REL      | #USR      | #DYNA       | PLACE                                                     | DURATION                         | INTERVAL |
| -------------------- | --------- | --------- | --------- | ----------- | --------------------------------------------------------- | -------------------------------- | -------- |
| Foursquare_TKY       | 94,890    | —         | 11,589    | 1,112,156   | Tokyo, Japan                                              | Apr. 3, 2012 -   Sep. 16, 2013   | —        |
| Foursquare_NYC       | 64,735    | —         | 17,175    | 568,444     | New York, USA                                             | Apr. 3, 2012 -   Sep. 16, 2013   | —        |
| Gowalla              | 1,280,969 | 913,660   | 107,092   | 6,442,892   | Global                                                    | Feb. 4, 2009 -   Oct. 23, 2010   | —        |
| BrightKite           | 772,966   | 394,334   | 51,406    | 4,747,287   | Global                                                    | Mar. 21, 2008 -   Oct. 18, 2010  | —        |
| Instagram            | 13,187    | —         | 78,233    | 2,205,794   | New York, USA                                             | Jun. 15, 2011 -   Nov. 8, 2016   | —        |


### Road Network Datasets

| DATASET              | #GEO      | #REL      | #USR      | #DYNA       | PLACE                                                     | DURATION                         | INTERVAL |
| -------------------- | --------- | --------- | --------- | ----------- | --------------------------------------------------------- | -------------------------------- | -------- |
| bj_roadmap_edge      | 38027     | 95660     | —         | —           | Beijing, China                                            | —                                | —        |
| bj_roadmap_node      | 16927     | 38027     | —         | —           | Beijing, China                                            | —                                | —        |


Note：

- NYCTAXI_DYNA is a dataset that counts the inflow and outflow of the region with an irregular area division method.
- NYCTAXI_OD is a dataset that counts the origin-destination flow between regions with an irregular area division method.
- NYCTAXI_GRID is a dataset that counts the inflow and outflow of the region with a grid-base division method.
- NYC_TOD is a dataset that counts the origin-destination flow between regions with a grid-base division method.

## Cite

Our paper is accepted by ACM SIGSPATIAL 2021. If you find LibCity useful for your research or development, please cite our [paper](https://dl.acm.org/doi/10.1145/3474717.3483923).

```
@inproceedings{10.1145/3474717.3483923,
  author = {Wang, Jingyuan and Jiang, Jiawei and Jiang, Wenjun and Li, Chao and Zhao, Wayne Xin},
  title = {LibCity: An Open Library for Traffic Prediction},
  year = {2021},
  isbn = {9781450386647},
  publisher = {Association for Computing Machinery},
  address = {New York, NY, USA},
  url = {https://doi.org/10.1145/3474717.3483923},
  doi = {10.1145/3474717.3483923},
  booktitle = {Proceedings of the 29th International Conference on Advances in Geographic Information Systems},
  pages = {145–148},
  numpages = {4},
  keywords = {Spatial-temporal System, Reproducibility, Traffic Prediction},
  location = {Beijing, China},
  series = {SIGSPATIAL '21}
}
```

```
Jingyuan Wang, Jiawei Jiang, Wenjun Jiang, Chao Li, and Wayne Xin Zhao. 2021. LibCity: An Open Library for Traffic Prediction. In Proceedings of the 29th International Conference on Advances in Geographic Information Systems (SIGSPATIAL '21). Association for Computing Machinery, New York, NY, USA, 145–148. DOI:https://doi.org/10.1145/3474717.3483923
```

