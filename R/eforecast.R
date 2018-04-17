#
# Run with:
#  source('eforecast.R')
#
#  forecasting after three cycles
#    eforecast("data/7252_reduced_data_day_min.csv",1,2,210)
#  two cycles more:
#    eforecast("data/7252_reduced_data_day_min.csv",1,4,210)
#
eforecast <- function(fname, Nweeks, Ndays, start_day){

library(forecast)

t <- read.csv(fname)

tdate <- as.Date(t$Col1day - 1, origin = "2009-01-01")
weekday <- weekdays(tdate)

t <- cbind(t,tdate)
t <- cbind(t,weekday)
# vector to aggregate per hour, since the data is taken every half and hour
hour <- c(1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23,24,24)
#yearmonth <- strftime(tdate, "%Y%m")

# one week starting in 201: Monday
#start_day <- 201
#Ndays <- 0;
    
par(mfrow=c(Nweeks,1))   
for( i in 1:Nweeks) {
   w <- t[t$Col1day>=start_day & t$Col1day<=(start_day+6),]
   wo <- w[order(w$Col1day),]

   wh <- c()
   for( j in 0:Ndays) {
     # select day
     day <- wo[wo$Col1day==start_day+j,]
     # sort day by minute
     dayo <- day[order(day$Col1min),]
     # sum every two points (half an hour)
     dayo <- cbind(dayo,hour)
     dayos <- aggregate(Col2~hour+Col0+Col1day+tdate+weekday, data=dayo, FUN=sum)
     wh <- rbind(wh,dayos)  
   }
   
   #print(wh)
   #print(wo)
   #dev.new()

   plot(wh$Col2, type='l')
   #plot(wh$weekday, wh$Col2, type='l')
   #plot(wh$tdate, wh$Col2, type='l')

   start_day <- start_day+7

}

sensor <- ts(wh$Col2,frequency=24)
    
fit <- auto.arima(sensor, max.p=3, d=0, max.q=1, max.P=0, D=1, max.Q=2)

fcast <- forecast(fit)

dev.new()
plot(fcast)


}
