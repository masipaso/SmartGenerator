### Examples of processing and forecasting energy data 

Check more examples in R: <br>
Forecasting: https://www.otexts.org/fpp/8/8 <br>
Seasonal Arima Models: https://www.otexts.org/fpp/8/9

### Energy data: <br>
Here it is used data samples used in one of the KNIME examples: <br>
https://www.knime.org/files/knime_bigdata_energy_timeseries_whitepaper.pdf <br>
https://www.knime.org/files/reducedenergydata.zip

this zip contains data of two meters: 7252 and 6496 (explanation about this data can be found in the pdf, page 8) <br>
7252_reduced_data.csv contains: <br>
"Col0","Col1","Col2"   <br>   
7252,19544,0.052 <br>
7252,19543,0.052  <br>
7252,19501,0.033  <br>
7252,19502,0.03 <br>
7252,19503,0.051 <br>
7252,19504,0.051 <br>
7252,19505,0.05 <br>
7252,19506,0.051 <br>
7252,19507,0.029 <br>
...  <br>

I used an script to convert it to :  <br>
Col0,Col1day,Col1min,Col2 <br>
7252,195,44,0.052 <br>
7252,195,43,0.052 <br>
7252,195,01,0.033 <br>
7252,195,02,0.03 <br>
7252,195,03,0.051 <br>
7252,195,04,0.051 <br>
7252,195,05,0.05 <br>
7252,195,06,0.051 <br>
7252,195,07,0.029 <br>
...  <br>

### econsume.R <br>
Shows how to aggregate the data in data/7252_reduced_data_day_min.csv or data/6496_reduced_data_day_min.csv <br>
and how to handle day and min, it plots aggregated data per hour

### eforecast.R <br>
Shows how to forecast day periods, plot forecasted periods

