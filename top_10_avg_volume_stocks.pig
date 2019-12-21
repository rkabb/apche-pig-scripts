/* 
This script finds the top 10 stock symbols with highest volumen for year 2003 .
Input and output locations are passed as parameter while running the script
RUN this script using below command

pig -param input=/user/hirw/input/stocks -param output=/user/hirwuser1330/output/pig/avg_volume top_10_avg_volume_stocks.pig


*/

/* LOad the data from hadoop cluster */

stocks = LOAD '$input' USING PigStorage(',') as ( exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float, volume:int, adj_close:float);

/* FILTER the data only for year 2003 */
filter_by_yr = FILTER stocks by GetYear(date) == 2003;

/* GROUP the data by symbol */
grp_by_sym = GROUP filter_by_yr By symbol;


/* Compute  the average by volume */
avg_volume = FOREACH grp_by_sym GENERATE  group , ROUND(AVG(filter_by_yr.volume)) as avgvolume;

/* Order the result set */
avg_vol_ordered = ORDER avg_volume BY avgvolume DESC;

/* Select only top 10 using LIMIT */
top10 = LIMIT avg_vol_ordered 10;


/* Write the output to the hadoop  output folder */
STORE top10 INTO '$output' using PigStorage(',');



