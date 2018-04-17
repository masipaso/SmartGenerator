# Run with:
# Source('econsume.R')
#
#  plot Nweeks starting in start_day, for each week plot Ndays
#  for example if day 201 is Monday, and Ndays is 2 and Nweks also 2 it will plot
#  Monday, Tuesday and Thursday for two week
#
t <- read.csv("C://Users//Julian//Documents//GitHub//SmartGenerator//R//data//7252_reduced_data_day_min.csv", header=TRUE, sep=",")


#Plot 6 days of 3 weeks, after week 201:
t <-    econsume(t,10,7,201)

#####################################################
# Read Data and tansform timestamp
####################################################
econsume <- function(fname, Nweeks, Ndays, start_day){
  
  #Converting column "Coliday" into date. Beginning with 01.01.2009. Wrting into "tdate"
  tdate <- as.Date(t$Col1day - 1, origin = "2009-01-01")
  #Extracting the weekdays from "tdate"
  weekday <- weekdays(tdate)
  
  #Combining dataset ("t") and "tdate" in one table
  t <- cbind(t,tdate)
  t <- cbind(t,weekday)
  # vector to aggregate per hour, since the data is taken every half and hour
  hour <- c(1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23,24,24)
  #yearmonth <- strftime(tdate, "%Y%m")
  
  # one week starting in 201: Monday
  #start_day <- 201
  #Ndays <- 0;
  #Grafik entsprechend aufteilen
  #par(mfrow=c(Nweeks,1))
  
  #For each Week
  for( i in 1:Nweeks) {
    #Write every day from startday to endday into "w"
    w <- t[t$Col1day>=start_day & t$Col1day<=(start_day + Ndays),]
    #Order these days
    wo <- w[order(w$Col1day),]
    #Create new empty matrix 
    wh <- c()
    #For ech Day (0 to 6)
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
    #plot(wh$Col2, type='l')
    #plot(wh$weekday, wh$Col2, type='l')
    #plot(wh$tdate, wh$Col2, type='l')
    
    start_day <- start_day+7
    
  }
  
  return(wh)
  
}

#####################################################
# Moving Average
#####################################################
# Calculate the moving average over NWeeks
movingAverage <- function(t){
  
  cma <- t$Col2 
  cma_a <- t$Col2
  cma_b <- t$Col2

tLength <- length(t$Col0)-12
for (i in 13: tLength ){
    # Calculate first part of the moving average
    aa <- 0 
    for (a in -12 : 11){
      aa = aa + as.numeric(t$Col2[i+a])
      #print(aa)
    }
    cma_a[i] <- (0.5*aa)/24
    
    # Calculate second part of the moving average
    bb <- 0
    for (b in -11 : 12){
      #print(i+ b)
      bb = bb + as.numeric(t$Col2[i+b])
    }
    cma_b[i] <- (0.5*bb)/24
    
    # Combine both parts with and write them into cma
    cma[i] <- cma_a[i]+cma_b[i]
}
return(cma)
}

# Call the function
cma <-  movingAverage(t)
# Plot the Moving Average
plot(cma, type ="l")

#####################################################
# Raw Index 
#####################################################
# Calculate the Raw Index over NWeeks
rawIndex <- function(t, cma){
  # How long is the array 
  tLength <- length(t$Col0)
  ri <- 0
  for (i in 1: tLength){
    # Divide all raw data points by the moving average
    ri[i] <- t$Col2[i]/cma[i]
  }
  return (ri)
}

# Call the function and write it into "ri"
ri <- rawIndex(t, cma)
# Plot the Raw Index
plot(ri, type = "l")

#####################################################
# Periodic Index (Calculate and normalize)
#####################################################
# Calculate the Periodic Index
periodicIndex <- function(t,ri){
# Declare tcma
tcma <- 0
# Combine t and cma
tcma <- cbind(t,ri)
# Declare a array for each weekday
pi_mo <- c()
pi_di <- c()
pi_mi <- c()
pi_do <- c()
pi_fr <- c()
pi_sa <- c()
pi_so <- c()
# How long is the array 
tLength <- length(tcma$Col0)
# Declare a counting variable for each weekday
i_mo <- 1
i_di <- 1
i_mi <- 1
i_do <- 1
i_fr <- 1
i_sa <- 1
i_so <- 1
# Separate each data point by weekday and write them into different arrays
for (i in 1: tLength){
  if (tcma$weekday[i] == "Montag"){
    pi_mo[i_mo] <- tcma$ri[i] 
    i_mo <- i_mo +1
  }
  else if (tcma$weekday[i] == "Dienstag"){
    pi_di[i_di] <- tcma$ri[i] 
    i_di <- i_di +1
  }
  else if (tcma$weekday[i] == "Mittwoch"){
    pi_mi[i_mi] <- tcma$ri[i] 
    i_mi <- i_mi +1
  }
  else if (tcma$weekday[i] == "Donnerstag"){
    pi_do[i_do] <- tcma$ri[i] 
    i_do <- i_do +1
  }
  else if (tcma$weekday[i] == "Freitag"){
    pi_fr[i_fr] <- tcma$ri[i] 
    i_fr <- i_fr +1
  }
  else if (tcma$weekday[i] == "Samstag"){
    pi_sa[i_sa] <- tcma$ri[i] 
    i_sa <- i_sa +1
  }
  else if (tcma$weekday[i] == "Sonntag"){
    pi_so[i_so] <- tcma$ri[i] 
    i_so <- i_so +1
  }
}



# Calculate the periodic index for every weekday

# Monday
pi_mo1 <- 0
for (j in 1: 24){
  for (i in 0: (length(pi_mo)/24-1)){
    pi_mo1[j] = 0.5*pi_mo[j+(i*24)]
  }
}

# Tuesday
pi_di1 <- 0
for (j in 1: 24){
  for (i in 0: (length(pi_di)/24-1)){
    pi_di1[j] = 0.5*pi_di[j+(i*24)]
  }
}


# Wednesday
pi_mi1 <- 0
for (j in 1: 24){
  for (i in 0: (length(pi_mi)/24-1)){
    pi_mi1[j] = 0.5*pi_mi[j+(i*24)]
  }
}


# Thursday
pi_do1 <- 0
for (j in 1: 24){
  for (i in 0: (length(pi_do)/24-1)){
    pi_do1[j] = 0.5*pi_do[j+(i*24)]
  }
}


# Friday
pi_fr1 <- 0
for (j in 1: 24){
  for (i in 0: (length(pi_fr)/24-1)){
    pi_fr1[j] = 0.5*pi_fr[j+(i*24)]
  }
}

# Saturday
pi_sa1 <- 0
for (j in 1: 24){
  for (i in 0: (length(pi_sa)/24-1)){
    pi_sa1[j] = 0.5*pi_sa[j+(i*24)]
  }
}

# Sunday
pi_so1 <- 0
for (j in 1: 24){
  for (i in 0: (length(pi_so)/24-1)){
    pi_so1[j] = 0.5*pi_so[j+(i*24)]
  }
}

# Combine all in one matrix
pi <- matrix(nrow = 24, ncol = 7)
pi[,1] <- pi_mo1
pi[,2] <- pi_di1
pi[,3] <- pi_mi1
pi[,4] <- pi_do1
pi[,5] <- pi_fr1
pi[,6] <- pi_sa1
pi[,7] <- pi_so1

return(pi)
}

# Call the function periodicIndex and write it into "pi"
pi <- periodicIndex(t, ri)
# Plot the Peridoc Index
plot(pi, type = "l")

# Normalize the Periodic Index
# Periodic Indices (PIs) should be 1.00
normalizePeriodicIndex <- function(pi){
  pi_no <- matrix(nrow = 24, ncol = 7)
 for (j in 1: 7){
   pi_sum <- 0
   for (i in 1: 24){
     for (k in 1: 24){
       # Calculate the sum of all PI for one weekday
       pi_sum <- pi_sum + pi[k,j]
     }
     # Divide PIs for each hour by the sum of all PIs for this weekday and multiply them by 24
       pi_no[i,j] <- 24*pi[i,j]/pi_sum
         }
  }
  return(pi_no)
}

# Call the function periodicIndex and write it into "pi"
pi_no <- normalizePeriodicIndex(pi)
# Plot the Peridoc Index
pi_no

#####################################################
# Adjusted time series
#####################################################
# Computes the adjusted time series by dividing all the data points in the time series with the
# adjusted PIs. The resulting time sries is used use them in the prediction method
adjustedTimeSeries <- function(t){
t_adj <- t
for (q in 1: length(t$hour)){
  if(t$weekday[q] == "Montag"){
    hour <- t$hour[q]
    t_adj$Col2[q] <- t_adj$Col2[q]/pi_no[hour,1]
  }
  else if(t$weekday[q] == "Dienstag"){
    hour <- t$hour[q]
    t_adj$Col2[q] <- t_adj$Col2[q]/pi_no[hour,2]
  }
  else if(t$weekday[q] == "Mittwoch"){
    hour <- t$hour[q]
    t_adj$Col2[q] <- t_adj$Col2[q]/pi_no[hour,3]
  }
  else if(t$weekday[q] == "Donnerstag"){
    hour <- t$hour[q]
    t_adj$Col2[q] <- t_adj$Col2[q]/pi_no[hour,4]
  }
  else if(t$weekday[q] == "Freitag"){
    hour <- t$hour[q]
    t_adj$Col2[q] <- t_adj$Col2[q]/pi_no[hour,5]
  }
  else if(t$weekday[q] == "Samstag"){
    hour <- t$hour[q]
    t_adj$Col2[q] <- t_adj$Col2[q]/pi_no[hour,6]
  }
  else if(t$weekday[q] == "Sonntag"){
    hour <- t$hour[q]
    t_adj$Col2[q] <- t_adj$Col2[q]/pi_no[hour,7]
  }
}
return(t_adj)
}

# Call the function periodicIndex and write it into "pi"
t_adj <- adjustedTimeSeries(pi)


#####################################################
# Prediction Method
#####################################################
# not ready yet.............
#sensor <- ts(wh$Col2,frequency=24)

#fit <- auto.arima(sensor, max.p=3, d=0, max.q=1, max.P=0, D=1, max.Q=2)

#fcast <- forecast(fit)

#dev.new()
#plot(fcast)

