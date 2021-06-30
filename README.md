# Dataset in LibTraffic

## Dataset Statistics Infomation

| DATASET        | #GEO      | #REL      | #USR    | #DYNA       | PLACE                       | DURATION                         | INTERVAL |
| -------------- | --------- | --------- | ------- | ----------- | --------------------------- | -------------------------------- | -------- |
| METR_LA        | 207       | 11,753    | —       | 7,094,304   | Los Angeles, USA            | Mar. 1, 2012 -   Jun. 27, 2012   | 5min     |
| LOS_LOOP       | 207       | 42,849    | —       | 7,094,304   | Los Angeles, USA            | Mar. 1, 2012 -   Jun. 27, 2012   | 5min     |
| SZ_TAXI        | 156       | 24,336    | —       | 464,256     | Shenzhen, China             | Jan. 1, 2015 -   Jan. 31, 2015   | 15min    |
| LOOP_SEATTLE   | 323       | 104,329   | —       | 33,953,760  | Greater Seattle   Area, USA | over the entirely   of 2015      | 5min     |
| Q_TRAFFIC      | 45,148    | 63,422    | —       | 264,386,688 | Beijing, China              | Apr. 1, 2017 - May 31, 2017      | 15min    |
| PEMSD3         | 358       | 547       | —       | 9,382,464   | California, USA             | Sept. 1, 2018 -   Nov. 30, 2018  | 5min     |
| PEMSD4         | 307       | 340       | —       | 5,216,544   | San Francisco Bay Area, USA | Jan. 1, 2018 -   Feb. 28, 2018   | 5min     |
| PEMSD7         | 883       | 866       | —       | 24,921,792  | California, USA             | Jul. 1, 2016 -   Aug. 31, 2016   | 5min     |
| PEMSD8         | 170       | 277       | —       | 3,035,520   | San Bernardino Area, USA    | Jul. 1, 2016 -   Aug. 31, 2016   | 5min     |
| PEMSD7(M)      | 228       | 51,984    | —       | 2,889,216   | California, USA             | weekdays of May   and June, 2012 | 5min     |
| PEMS_BAY       | 325       | 8,358     | —       | 16,937,700  | San Francisco Bay Area, USA | Jan. 1, 2017 -   Jun. 30, 2017   | 5min     |
| BEIJING_SUBWAY | 276       | 76,176    | —       | 248,400     | Beijing, China              | Feb. 29, 2016 -   Apr. 3, 2016   | 30min    |
| M_DENSE        | 30        | —         | —       | 525,600     | Madrid, Spain               | Jan. 1, 2018 -   Dec. 21, 2019   | 60min    |
| ROTTERDAM      | 208       | —         | —       | 4,813,536   | Rotterdam, Holland          | 135 days of 2018                 | 2min     |
| SHMETRO        | 288       | 82,944    | —       | 1,934,208   | Shanghai, China             | Jul. 1, 2016 -   Sept. 30, 2016  | 15min    |
| HZMETRO        | 80        | 6,400     | —       | 146,000     | Hangzhou, China             | Jan. 1, 2019 - Jan. 25, 2019     | 15min    |
| TaxiBJ         | 32*32     | —         | —       | 5,652,480   | Beijing, China              | Mar. 1, 2015 -   Jun. 30, 2015   | 30min    |
| T-Drive        | 32*32     | 1,048,576 | —       | 3,686,400   | Beijing, China              | Feb. 1, 2015 -   Jun. 30, 2015   | 60min    |
| Porto          | 20*10     | —         | —       | 441,600     | Porto, Portugal             | Jul. 1, 2013 -   Sept. 30, 2013  | 60min    |
| NYC-Taxi-Dyna  | 263       | 69,169    | —       | 574,392     | New York, USA               | Jan. 1, 2020 - Mar. 30, 2020     | 60min    |
| NYC-Taxi-OD    | 263       | 69,169    | —       | 150,995,927 | New York, USA               | Apr. 1, 2020 - Jun. 30, 2020     | 60min    |
| NYC-Taxi-Grid  | 10*20     | —         | —       | 432,000     | New York, USA               | Jan. 1, 2014 -   Mar. 31, 2014   | 60min    |
| NYC-Bike       | 10*20     | —         | —       | 441,600     | New York, USA               | Jul. 1, 2020 -   Sept. 30, 2020  | 60min    |
| AustinRide     | 16*8      | —         | —       | 282,624     | Austin, USA                 | Jul. 1, 2016 -   Sept. 30, 2016  | 60min    |
| BikeDC         | 16*8      | —         | —       | 282,624     | Washington, USA             | Jul. 1, 2020 -   Sept. 30, 2020  | 60min    |
| BikeCHI        | 15*18     | —         | —       | 670,680     | Chicago, USA                | Jul. 1, 2020 -   Sept. 30, 2020  | 60min    |
| Foursquare-TKY | 94,890    | —         | 11,589  | 1,112,156   | Tokyo, Japan                | Apr. 3, 2012 -   Sep. 16, 2013   | —        |
| Foursquare-NYC | 64,735    | —         | 17,175  | 568,444     | New York, USA               | Apr. 3, 2012 -   Sep. 16, 2013   | —        |
| Gowalla        | 1,280,969 | 913,660   | 107,092 | 6,442,892   | Global                      | Feb. 4, 2009 -   Oct. 23, 2010   | —        |
| BrightKite     | 772,966   | 394,334   | 51,406  | 4,747,287   | Global                      | Mar. 21, 2008 -   Oct. 18, 2010  | —        |
| Instagram      | 13,187    | —         | 78,233  | 2,205,794   | New York, USA               | Jun. 15, 2011 -   Nov. 8, 2016   | —        |

Note：

- NYC-Taxi-Dyna is a dataset that counts the inflow and outflow of the region with an irregular area division method.
- NYC-Taxi-OD is a dataset that counts the origin-destination flow between regions with an irregular area division method.
- NYC-Taxi-Grid is a dataset that counts the inflow and outflow of the region with a grid-base division method.