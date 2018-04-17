package org.apache.flink;


import com.workday.insights.timeseries.arima.struct.ArimaParams;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import data.KeyedDataPoint;
import functions.MovingAverageFunction;
import org.apache.flink.api.java.tuple.Tuple;
import com.workday.insights.timeseries.arima.Arima;
import com.workday.insights.timeseries.arima.struct.ForecastResult;

import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.util.*;

import sinks.InfluxDBSink;


public class Preparator {

    //
    //	Program
    //

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define TimeCharacteristic as EventTime if Data already has timestamp
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        @SuppressWarnings({"rawtypes", "serial"})

        DataStream<KeyedDataPoint<Double>> energyData_seed1;
        // Read the data from file
        energyData_seed1 = env.readTextFile("src/main/resources/6496_reduced_data.csv")
                // energyData = env.readTextFile("src/main/resources/tmp.csv")
                // transforms dara via map function --> split data and assigns timestamp and key to it
                .map(new ParseData())
                .assignTimestampsAndWatermarks(new ExtractTimestamp());

        //energyData.print()
        //
        //energyData_seed1.addSink(new InfluxDBSink<>("rawData_6496"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Moving average over 2 hours each
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Combine all the streams into one and write it to Kafka
        DataStream<KeyedDataPoint<Double>> ma_energyData_seed1 = energyData_seed1

                .keyBy(new KeySelector<KeyedDataPoint<Double>, String>(){
                           @Override
                           public String getKey(KeyedDataPoint<Double> doubleKeyedDataPoint) throws Exception {
                               return doubleKeyedDataPoint.getKey();
                           }
                       }
                )
                //
                // In energy data we have one measurement every half an hour of the day
                // so here I am taking windows of 120 minutes <- this should contain then 4 points
                // so the moving average is done every two hours
                .window(SlidingEventTimeWindows.of(Time.minutes(120), Time.minutes(30))).trigger(CountTrigger.of(4))
                // Apply the moving average to the 120 minutes windows
                .apply(new MovingAverageFunction());
        //.name("energyDataAvg");

        //ma_energyData.print();

        //ma_energyData_seed1.addSink(new InfluxDBSink<>("movingAverage_seed1"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Raw Index for each hour
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        DataStream<KeyedDataPoint<Double>> engergyData_raw_ma_seed1 = energyData_seed1
                .keyBy("key")
                .join(ma_energyData_seed1)
                .where(new TimestampKeySelector())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new DivideValueJoinedStreams());


        //engergyData_raw_ma.print();
        //engergyData_raw_ma_seed1.addSink(new InfluxDBSink<>("rawIndex_seed1"));



        // Store Periodic Index for each hour of each day of the week

        DataStream<Tuple3<Integer, Integer, Double>> pi_mondays_seed1 =
                engergyData_raw_ma_seed1
                        // we use tuple9 to expand the timestamp to be able to compare the hours and the days of the week
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(1))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        DataStream<Tuple3<Integer, Integer, Double>> pi_tuesdays_seed1 =
                engergyData_raw_ma_seed1
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(2))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        DataStream<Tuple3<Integer, Integer, Double>> pi_wednesdays_seed1 =
                engergyData_raw_ma_seed1
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(3))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        DataStream<Tuple3<Integer, Integer, Double>> pi_thursdays_seed1 =
                engergyData_raw_ma_seed1
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(4))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        DataStream<Tuple3<Integer, Integer, Double>> pi_fridays_seed1 =
                engergyData_raw_ma_seed1
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(5))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        DataStream<Tuple3<Integer, Integer, Double>> pi_saturdays_seed1 =
                engergyData_raw_ma_seed1
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(6))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        DataStream<Tuple3<Integer, Integer, Double>> pi_sundays_seed1 =
                engergyData_raw_ma_seed1
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(7))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        //.print();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PEAKS  1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<KeyedDataPoint<Double>> peaks_mondays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                //.filter(new FilterDay(1))
                .join(pi_mondays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_tuesdays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_tuesdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_wednesdays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_wednesdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_thursdays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_thursdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_fridays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_fridays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_saturdays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_saturdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_sundays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_sundays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        peaks_mondays_seed1 = peaks_mondays_seed1.filter(new FilterKey("6496"));
        peaks_tuesdays_seed1 = peaks_tuesdays_seed1.filter(new FilterKey("6496"));
        peaks_wednesdays_seed1 = peaks_wednesdays_seed1.filter(new FilterKey("6496"));
        peaks_thursdays_seed1 = peaks_thursdays_seed1.filter(new FilterKey("6496"));
        peaks_fridays_seed1 = peaks_fridays_seed1.filter(new FilterKey("6496"));
        peaks_saturdays_seed1 = peaks_saturdays_seed1.filter(new FilterKey("6496"));
        peaks_sundays_seed1 = peaks_sundays_seed1.filter(new FilterKey("6496"));
        //peaks_sundays_seed2.print();

        DataStream<KeyedDataPoint<Double>> peaks1;
        peaks1 = peaks_mondays_seed1.union(peaks_tuesdays_seed1).union(peaks_wednesdays_seed1).union(peaks_thursdays_seed1).union(peaks_fridays_seed1).union(peaks_saturdays_seed1).union(peaks_sundays_seed1);
        //peaks1.addSink(new InfluxDBSink<>("peaks1"));


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Adjusted Time Series for each day ot the week
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<KeyedDataPoint<Double>> ats_mondays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_mondays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_tuesdays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_tuesdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_wednesdays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_wednesdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_thursdays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_thursdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_fridays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_fridays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_saturdays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_saturdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_sundays_seed1 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_sundays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        ats_mondays_seed1 = ats_mondays_seed1.filter(new FilterKey("6496"));
        ats_tuesdays_seed1 = ats_tuesdays_seed1.filter(new FilterKey("6496"));
        ats_wednesdays_seed1 = ats_wednesdays_seed1.filter(new FilterKey("6496"));
        ats_thursdays_seed1 = ats_thursdays_seed1.filter(new FilterKey("6496"));
        ats_fridays_seed1 = ats_fridays_seed1.filter(new FilterKey("6496"));
        ats_saturdays_seed1 = ats_saturdays_seed1.filter(new FilterKey("6496"));
        ats_sundays_seed1 = ats_sundays_seed1.filter(new FilterKey("6496"));


        //ats_sundays_seed2.print();
        //ats_sundays_seed2.addSink(new InfluxDBSink<>("ats_sundays_seed2"));
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Adjusted Time Series
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<KeyedDataPoint<Double>> ats1;
        ats1 = ats_mondays_seed1.union(ats_tuesdays_seed1).union(ats_wednesdays_seed1).union(ats_thursdays_seed1).union(ats_fridays_seed1).union(ats_saturdays_seed1).union(ats_sundays_seed1);
        //ats1.addSink(new InfluxDBSink<>("ats1"));



        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // SEED 2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<KeyedDataPoint<Double>> energyData_seed2;
        // Read the data from file
        energyData_seed2 = env.readTextFile("src/main/resources/7252_reduced_data.csv")
                // energyData = env.readTextFile("src/main/resources/tmp.csv")
                // transforms dara via map function --> split data and assigns timestamp and key to it
                .map(new ParseData())
                .assignTimestampsAndWatermarks(new ExtractTimestamp());

        //energyData.print();
        //energyData_seed2.addSink(new InfluxDBSink<>("rawData_7252"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Moving average over 2 hours each
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Combine all the streams into one and write it to Kafka
        DataStream<KeyedDataPoint<Double>> ma_energyData_seed2 = energyData_seed2

                .keyBy(new KeySelector<KeyedDataPoint<Double>, String>(){
                           @Override
                           public String getKey(KeyedDataPoint<Double> doubleKeyedDataPoint) throws Exception {
                               return doubleKeyedDataPoint.getKey();
                           }
                       }
                )
                //
                // In energy data we have one measurement every half an hour of the day
                // so here I am taking windows of 120 minutes <- this should contain then 4 points
                // so the moving average is done every two hours
                .window(SlidingEventTimeWindows.of(Time.minutes(120), Time.minutes(30))).trigger(CountTrigger.of(4))
                // Apply the moving average to the 120 minutes windows
                .apply(new MovingAverageFunction());
        //.name("energyDataAvg");

        //ma_energyData.print();

        //ma_energyData_seed2.addSink(new InfluxDBSink<>("movingAverage_seed2"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Raw Index for each hour
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        DataStream<KeyedDataPoint<Double>> engergyData_raw_ma_seed2 = energyData_seed2
                .keyBy("key")
                .join(ma_energyData_seed2)
                .where(new TimestampKeySelector())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new DivideValueJoinedStreams());


        //engergyData_raw_ma.print();
        //engergyData_raw_ma_seed2.addSink(new InfluxDBSink<>("rawIndex_seed2"));


        //DataStream<KeyedDataPoint<Double>>  mondays =
        /*
    	engergyData_raw_ma
    	    .map(new ExpandDateCalendar())
    	    //.filter(new FilterDay(1))
    	    .keyBy(4)   // key by the hour
    	    .window(TumblingEventTimeWindows.of(Time.days(10000)))
    		.apply(new AveragePerDay())

    	   // .keyBy(fields)
    	   // .map(new BackToPoint());
    	    //.keyBy(6)
    	    //.apply()
    	   .print();
    	*/
        // mondays.print();


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PERIODIC INDEX FOR EACH WEEK DAY (7)
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<Tuple3<Integer, Integer, Double>> pi_mondays_seed2 =
                engergyData_raw_ma_seed2
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(1))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        DataStream<Tuple3<Integer, Integer, Double>> pi_tuesdays_seed2 =
                engergyData_raw_ma_seed2
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(2))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        DataStream<Tuple3<Integer, Integer, Double>> pi_wednesdays_seed2 =
                engergyData_raw_ma_seed2
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(3))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        DataStream<Tuple3<Integer, Integer, Double>> pi_thursdays_seed2 =
                engergyData_raw_ma_seed2
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(4))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        DataStream<Tuple3<Integer, Integer, Double>> pi_fridays_seed2 =
                engergyData_raw_ma_seed2
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(5))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        DataStream<Tuple3<Integer, Integer, Double>> pi_saturdays_seed2 =
                engergyData_raw_ma_seed2
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(6))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        DataStream<Tuple3<Integer, Integer, Double>> pi_sundays_seed2 =
                engergyData_raw_ma_seed2
                        .map(new ExpandDateCalendar())
                        .filter(new FilterDay(7))
                        .keyBy(4)   // key by the hour
                        .window(TumblingEventTimeWindows.of(Time.days(10000)))
                        .apply(new AveragePerDay());

        //.print();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PEAKS
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<KeyedDataPoint<Double>> peaks_mondays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                //.filter(new FilterDay(1))
                .join(pi_mondays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_tuesdays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_tuesdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_wednesdays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_wednesdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_thursdays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_thursdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_fridays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_fridays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_saturdays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_saturdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_sundays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_sundays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        peaks_mondays_seed2 = peaks_mondays_seed2.filter(new FilterKey("7252"));
        peaks_tuesdays_seed2 = peaks_tuesdays_seed2.filter(new FilterKey("7252"));
        peaks_wednesdays_seed2 = peaks_wednesdays_seed2.filter(new FilterKey("7252"));
        peaks_thursdays_seed2 = peaks_thursdays_seed2.filter(new FilterKey("7252"));
        peaks_fridays_seed2 = peaks_fridays_seed2.filter(new FilterKey("7252"));
        peaks_saturdays_seed2 = peaks_saturdays_seed2.filter(new FilterKey("7252"));
        peaks_sundays_seed2 = peaks_sundays_seed2.filter(new FilterKey("7252"));
        //peaks_sundays_seed2.print();

        DataStream<KeyedDataPoint<Double>> peaks2;
        peaks2 = peaks_mondays_seed2.union(peaks_tuesdays_seed2).union(peaks_wednesdays_seed2).union(peaks_thursdays_seed2).union(peaks_fridays_seed2).union(peaks_saturdays_seed2).union(peaks_sundays_seed2);
        //peaks2.addSink(new InfluxDBSink<>("peaks2"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Adjusted Time Series for each day of the week
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<KeyedDataPoint<Double>> ats_mondays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_mondays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_tuesdays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_tuesdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_wednesdays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_wednesdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_thursdays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_thursdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_fridays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_fridays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_saturdays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_saturdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_sundays_seed2 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_sundays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        ats_mondays_seed2 = ats_mondays_seed2.filter(new FilterKey("7252"));
        ats_tuesdays_seed2 = ats_tuesdays_seed2.filter(new FilterKey("7252"));
        ats_wednesdays_seed2 = ats_wednesdays_seed2.filter(new FilterKey("7252"));
        ats_thursdays_seed2 = ats_thursdays_seed2.filter(new FilterKey("7252"));
        ats_fridays_seed2 = ats_fridays_seed2.filter(new FilterKey("7252"));
        ats_saturdays_seed2 = ats_saturdays_seed2.filter(new FilterKey("7252"));
        ats_sundays_seed2 = ats_sundays_seed2.filter(new FilterKey("7252"));


        //ats_sundays_seed2.print();
        //ats_sundays_seed2.addSink(new InfluxDBSink<>("ats_sundays_seed2"));
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Adjusted Time Series
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<KeyedDataPoint<Double>> ats2;
        ats2 = ats_mondays_seed2.union(ats_tuesdays_seed2).union(ats_wednesdays_seed2).union(ats_thursdays_seed2).union(ats_fridays_seed2).union(ats_saturdays_seed2).union(ats_sundays_seed2);
        //ats2.addSink(new InfluxDBSink<>("ats2"));

        //ats.map(new ExpandDateCalendar()).print();
        //ats.addSink(new InfluxDBSink<>("ats_seed2"));



        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PEAKS  2.1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<KeyedDataPoint<Double>> peaks_mondays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                //.filter(new FilterDay(1))
                .join(pi_mondays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_tuesdays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_tuesdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_wednesdays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_wednesdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_thursdays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_thursdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_fridays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_fridays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_saturdays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_saturdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_sundays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_sundays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        peaks_mondays_seed21 = peaks_mondays_seed21.filter(new FilterKey("7252"));
        peaks_tuesdays_seed21 = peaks_tuesdays_seed21.filter(new FilterKey("7252"));
        peaks_wednesdays_seed21 = peaks_wednesdays_seed21.filter(new FilterKey("7252"));
        peaks_thursdays_seed21 = peaks_thursdays_seed21.filter(new FilterKey("7252"));
        peaks_fridays_seed21 = peaks_fridays_seed21.filter(new FilterKey("7252"));
        peaks_saturdays_seed21 = peaks_saturdays_seed21.filter(new FilterKey("7252"));
        peaks_sundays_seed21 = peaks_sundays_seed21.filter(new FilterKey("7252"));
        //peaks_sundays_seed2.print();

        DataStream<KeyedDataPoint<Double>> peaks21;
        peaks21 = peaks_mondays_seed21.union(peaks_tuesdays_seed21).union(peaks_wednesdays_seed21).union(peaks_thursdays_seed21).union(peaks_fridays_seed21).union(peaks_saturdays_seed21).union(peaks_sundays_seed21);
        //peaks21.addSink(new InfluxDBSink<>("peaks21"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Adjusted Time Series 2.1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<KeyedDataPoint<Double>> ats_mondays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_mondays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_tuesdays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_tuesdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_wednesdays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_wednesdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_thursdays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_thursdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_fridays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_fridays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_saturdays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_saturdays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_sundays_seed21 = energyData_seed2
                .map(new ExpandDateCalendar())
                .join(pi_sundays_seed1)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        ats_mondays_seed21 = ats_mondays_seed21.filter(new FilterKey("7252"));
        ats_tuesdays_seed21 = ats_tuesdays_seed21.filter(new FilterKey("7252"));
        ats_wednesdays_seed21 = ats_wednesdays_seed21.filter(new FilterKey("7252"));
        ats_thursdays_seed21 = ats_thursdays_seed21.filter(new FilterKey("7252"));
        ats_fridays_seed21 = ats_fridays_seed21.filter(new FilterKey("7252"));
        ats_saturdays_seed21 = ats_saturdays_seed21.filter(new FilterKey("7252"));
        ats_sundays_seed21 = ats_sundays_seed21.filter(new FilterKey("7252"));


        //ats_sundays_seed2.print();
        //ats_sundays_seed2.addSink(new InfluxDBSink<>("ats_sundays_seed2"));
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Adjusted Time Series 2.1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<KeyedDataPoint<Double>> ats21;
        ats21 = ats_mondays_seed21.union(ats_tuesdays_seed21).union(ats_wednesdays_seed21).union(ats_thursdays_seed21).union(ats_fridays_seed21).union(ats_saturdays_seed21).union(ats_sundays_seed21);
        //ats21.addSink(new InfluxDBSink<>("ats21"));



        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PEAKS  1.2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<KeyedDataPoint<Double>> peaks_mondays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                //.filter(new FilterDay(1))
                .join(pi_mondays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_tuesdays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_tuesdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_wednesdays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_wednesdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_thursdays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_thursdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_fridays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_fridays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_saturdays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_saturdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        DataStream<KeyedDataPoint<Double>> peaks_sundays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_sundays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new Peaks());

        peaks_mondays_seed12 = peaks_mondays_seed12.filter(new FilterKey("6496"));
        peaks_tuesdays_seed12 = peaks_tuesdays_seed12.filter(new FilterKey("6496"));
        peaks_wednesdays_seed12 = peaks_wednesdays_seed12.filter(new FilterKey("6496"));
        peaks_thursdays_seed12 = peaks_thursdays_seed12.filter(new FilterKey("6496"));
        peaks_fridays_seed12 = peaks_fridays_seed12.filter(new FilterKey("6496"));
        peaks_saturdays_seed12 = peaks_saturdays_seed12.filter(new FilterKey("6496"));
        peaks_sundays_seed12 = peaks_sundays_seed12.filter(new FilterKey("6496"));
        //peaks_sundays_seed2.print();

        DataStream<KeyedDataPoint<Double>> peaks12;
        peaks12 = peaks_mondays_seed12.union(peaks_tuesdays_seed12).union(peaks_wednesdays_seed12).union(peaks_thursdays_seed12).union(peaks_fridays_seed12).union(peaks_saturdays_seed12).union(peaks_sundays_seed12);
        //peaks12.addSink(new InfluxDBSink<>("peaks12"));



        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Adjusted Time Series 1.2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<KeyedDataPoint<Double>> ats_mondays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_mondays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_tuesdays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_tuesdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_wednesdays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_wednesdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_thursdays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_thursdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_fridays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_fridays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_saturdays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_saturdays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        DataStream<KeyedDataPoint<Double>> ats_sundays_seed12 = energyData_seed1
                .map(new ExpandDateCalendar())
                .join(pi_sundays_seed2)
                .where(new DayOfWeekSelectorTuple9())
                .equalTo(new DayOfWeekSelectorTuple3())
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new AdjustedTimeSeries());

        ats_mondays_seed12 = ats_mondays_seed12.filter(new FilterKey("6496"));
        ats_tuesdays_seed12 = ats_tuesdays_seed12.filter(new FilterKey("6496"));
        ats_wednesdays_seed12 = ats_wednesdays_seed12.filter(new FilterKey("6496"));
        ats_thursdays_seed12 = ats_thursdays_seed12.filter(new FilterKey("6496"));
        ats_fridays_seed12 = ats_fridays_seed12.filter(new FilterKey("6496"));
        ats_saturdays_seed12 = ats_saturdays_seed12.filter(new FilterKey("6496"));
        ats_sundays_seed12 = ats_sundays_seed12.filter(new FilterKey("6496"));


        //ats_sundays_seed2.addSink(new InfluxDBSink<>("ats_sundays_seed2"));



        DataStream<KeyedDataPoint<Double>> ats12;
        ats12 = ats_mondays_seed12.union(ats_tuesdays_seed12).union(ats_wednesdays_seed12).union(ats_thursdays_seed12).union(ats_fridays_seed12).union(ats_saturdays_seed12).union(ats_sundays_seed12);
        //ats12.addSink(new InfluxDBSink<>("ats12"));

        /*
        This is the arima from mondays frome the first seed combined with the periodic index for mondays for the second seed
        which gave us the adjusted time series 12
        Then added the peaks calling the SubstractValueJoinedStreams function
        do this for all the ats, in total 28 (7x4)
        to merge together all the days of the week in one stream use union as used above
        */

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ARIMA FORECASTED DATA 1.2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<Tuple3<String, Long, Double>> arima_mondays12;
        arima_mondays12 = ats_mondays_seed12
                // we pass to tuple3 to use its method compareTo in order to sort the tuples to pass it to the arima function
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_tuesdays12;
        arima_tuesdays12 = ats_tuesdays_seed12
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_wednesdays12;
        arima_wednesdays12 = ats_wednesdays_seed12
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_thursdays12;
        arima_thursdays12 = ats_thursdays_seed12
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_fridays12;
        arima_fridays12 = ats_fridays_seed12
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_saturdays12;
        arima_saturdays12 = ats_saturdays_seed12
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_sundays12;
        arima_sundays12 = ats_sundays_seed12
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ARIMA FORECASTED DATA 1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<Tuple3<String, Long, Double>> arima_mondays1;
        arima_mondays1 = ats_mondays_seed1
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_tuesdays1;
        arima_tuesdays1 = ats_tuesdays_seed1
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_wednesdays1;
        arima_wednesdays1 = ats_wednesdays_seed1
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_thursdays1;
        arima_thursdays1 = ats_thursdays_seed1
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_fridays1;
        arima_fridays1 = ats_fridays_seed1
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_saturdays1;
        arima_saturdays1 = ats_saturdays_seed1
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_sundays1;
        arima_sundays1 = ats_sundays_seed1
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ARIMA FORECASTED DATA 2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<Tuple3<String, Long, Double>> arima_mondays2;
        arima_mondays2 = ats_mondays_seed2
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_tuesdays2;
        arima_tuesdays2 = ats_tuesdays_seed2
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_wednesdays2;
        arima_wednesdays2 = ats_wednesdays_seed2
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_thursdays2;
        arima_thursdays2 = ats_thursdays_seed2
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_fridays2;
        arima_fridays2 = ats_fridays_seed2
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_saturdays2;
        arima_saturdays2 = ats_saturdays_seed2
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_sundays2;
        arima_sundays2 = ats_sundays_seed2
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ARIMA FORECASTED DATA 2.1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        DataStream<Tuple3<String, Long, Double>> arima_mondays21;
        arima_mondays21 = ats_mondays_seed21
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_tuesdays21;
        arima_tuesdays21 = ats_tuesdays_seed21
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_wednesdays21;
        arima_wednesdays21 = ats_wednesdays_seed21
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_thursdays21;
        arima_thursdays21 = ats_thursdays_seed21
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_fridays21;
        arima_fridays21 = ats_fridays_seed21
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_saturdays21;
        arima_saturdays21 = ats_saturdays_seed21
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());

        DataStream<Tuple3<String, Long, Double>> arima_sundays21;
        arima_sundays21 = ats_sundays_seed21
                .map(new BackToTuple3())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(10000)))
                .apply(new ArimaFunction());


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 1 TO 1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays1to1 = arima_mondays1
                .keyBy(1)
                .join(peaks_mondays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays1to1 = arima_tuesdays1
                .keyBy(1)
                .join(peaks_tuesdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays1to1 = arima_wednesdays1
                .keyBy(1)
                .join(peaks_wednesdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays1to1 = arima_thursdays1
                .keyBy(1)
                .join(peaks_thursdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays1to1 = arima_fridays1
                .keyBy(1)
                .join(peaks_fridays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays1to1 = arima_saturdays1
                .keyBy(1)
                .join(peaks_saturdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays1to1 = arima_sundays1
                .keyBy(1)
                .join(peaks_sundays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks1to1;
        addpeaks1to1 = addpeaks_mondays1to1.union(addpeaks_tuesdays1to1).union(addpeaks_wednesdays1to1).union(addpeaks_thursdays1to1).union(addpeaks_fridays1to1).union(addpeaks_saturdays1to1).union(addpeaks_sundays1to1);
        addpeaks1to1.addSink(new InfluxDBSink<>("addpeaks1"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 1 TO 1.2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays1to12 = arima_mondays1
                .keyBy(1)
                .join(peaks_mondays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays1to12 = arima_tuesdays1
                .keyBy(1)
                .join(peaks_tuesdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays1to12 = arima_wednesdays1
                .keyBy(1)
                .join(peaks_wednesdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays1to12 = arima_thursdays1
                .keyBy(1)
                .join(peaks_thursdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays1to12 = arima_fridays1
                .keyBy(1)
                .join(peaks_fridays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays1to12 = arima_saturdays1
                .keyBy(1)
                .join(peaks_saturdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays1to12 = arima_sundays1
                .keyBy(1)
                .join(peaks_sundays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks1to12;
        addpeaks1to12 = addpeaks_mondays1to12.union(addpeaks_tuesdays1to12).union(addpeaks_wednesdays1to12).union(addpeaks_thursdays1to12).union(addpeaks_fridays1to12).union(addpeaks_saturdays1to12).union(addpeaks_sundays1to12);
        addpeaks1to12.addSink(new InfluxDBSink<>("addpeaks2"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 1 TO 2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays1to2 = arima_mondays1
                .keyBy(1)
                .join(peaks_mondays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays1to2 = arima_tuesdays1
                .keyBy(1)
                .join(peaks_tuesdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays1to2 = arima_wednesdays1
                .keyBy(1)
                .join(peaks_wednesdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays1to2 = arima_thursdays1
                .keyBy(1)
                .join(peaks_thursdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays1to2 = arima_fridays1
                .keyBy(1)
                .join(peaks_fridays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays1to2 = arima_saturdays1
                .keyBy(1)
                .join(peaks_saturdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays1to2 = arima_sundays1
                .keyBy(1)
                .join(peaks_sundays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks1to2;
        addpeaks1to2 = addpeaks_mondays1to2.union(addpeaks_tuesdays1to2).union(addpeaks_wednesdays1to2).union(addpeaks_thursdays1to2).union(addpeaks_fridays1to2).union(addpeaks_saturdays1to2).union(addpeaks_sundays1to2);
        addpeaks1to2.addSink(new InfluxDBSink<>("addpeaks3"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 1 TO 2.1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays1to21 = arima_mondays1
                .keyBy(1)
                .join(peaks_mondays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays1to21 = arima_tuesdays1
                .keyBy(1)
                .join(peaks_tuesdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays1to21 = arima_wednesdays1
                .keyBy(1)
                .join(peaks_wednesdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays1to21 = arima_thursdays1
                .keyBy(1)
                .join(peaks_thursdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays1to21 = arima_fridays1
                .keyBy(1)
                .join(peaks_fridays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays1to21 = arima_saturdays1
                .keyBy(1)
                .join(peaks_saturdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays1to21 = arima_sundays1
                .keyBy(1)
                .join(peaks_sundays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks1to21;
        addpeaks1to21 = addpeaks_mondays1to21.union(addpeaks_tuesdays1to21).union(addpeaks_wednesdays1to21).union(addpeaks_thursdays1to21).union(addpeaks_fridays1to21).union(addpeaks_saturdays1to21).union(addpeaks_sundays1to21);
        addpeaks1to21.addSink(new InfluxDBSink<>("addpeaks4"));
/*
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 1.2 TO 1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays12to1 = arima_mondays12
                .keyBy(1)
                .join(peaks_mondays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays12to1 = arima_tuesdays12
                .keyBy(1)
                .join(peaks_tuesdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays12to1 = arima_wednesdays12
                .keyBy(1)
                .join(peaks_wednesdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays12to1 = arima_thursdays12
                .keyBy(1)
                .join(peaks_thursdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays12to1 = arima_fridays12
                .keyBy(1)
                .join(peaks_fridays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays12to1 = arima_saturdays12
                .keyBy(1)
                .join(peaks_saturdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays12to1 = arima_sundays12
                .keyBy(1)
                .join(peaks_sundays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks12to1;
        addpeaks12to1 = addpeaks_mondays12to1.union(addpeaks_tuesdays12to1).union(addpeaks_wednesdays12to1).union(addpeaks_thursdays12to1).union(addpeaks_fridays12to1).union(addpeaks_saturdays12to1).union(addpeaks_sundays12to1);
        addpeaks12to1.addSink(new InfluxDBSink<>("addpeaks5"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 1.2 TO 1.2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays12to12 = arima_mondays12
                .keyBy(1)
                .join(peaks_mondays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays12to12 = arima_tuesdays12
                .keyBy(1)
                .join(peaks_tuesdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays12to12 = arima_wednesdays12
                .keyBy(1)
                .join(peaks_wednesdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays12to12 = arima_thursdays12
                .keyBy(1)
                .join(peaks_thursdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays12to12 = arima_fridays12
                .keyBy(1)
                .join(peaks_fridays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays12to12 = arima_saturdays12
                .keyBy(1)
                .join(peaks_saturdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays12to12 = arima_sundays12
                .keyBy(1)
                .join(peaks_sundays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks12to12;
        addpeaks12to12 = addpeaks_mondays12to12.union(addpeaks_tuesdays12to12).union(addpeaks_wednesdays12to12).union(addpeaks_thursdays12to12).union(addpeaks_fridays12to12).union(addpeaks_saturdays12to12).union(addpeaks_sundays12to12);
        addpeaks12to12.addSink(new InfluxDBSink<>("addpeaks6"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 1.2 TO 2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays12to2 = arima_mondays12
                .keyBy(1)
                .join(peaks_mondays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays12to2 = arima_tuesdays12
                .keyBy(1)
                .join(peaks_tuesdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays12to2 = arima_wednesdays12
                .keyBy(1)
                .join(peaks_wednesdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays12to2 = arima_thursdays12
                .keyBy(1)
                .join(peaks_thursdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays12to2 = arima_fridays12
                .keyBy(1)
                .join(peaks_fridays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays12to2 = arima_saturdays12
                .keyBy(1)
                .join(peaks_saturdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays12to2 = arima_sundays12
                .keyBy(1)
                .join(peaks_sundays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks12to2;
        addpeaks12to2 = addpeaks_mondays12to2.union(addpeaks_tuesdays12to2).union(addpeaks_wednesdays12to2).union(addpeaks_thursdays12to2).union(addpeaks_fridays12to2).union(addpeaks_saturdays12to2).union(addpeaks_sundays12to2);
        addpeaks12to2.addSink(new InfluxDBSink<>("addpeaks7"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 1.2 TO 2.1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays12to21 = arima_mondays12
                .keyBy(1)
                .join(peaks_mondays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays12to21 = arima_tuesdays12
                .keyBy(1)
                .join(peaks_tuesdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays12to21 = arima_wednesdays12
                .keyBy(1)
                .join(peaks_wednesdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays12to21 = arima_thursdays12
                .keyBy(1)
                .join(peaks_thursdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays12to21 = arima_fridays12
                .keyBy(1)
                .join(peaks_fridays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays12to21 = arima_saturdays12
                .keyBy(1)
                .join(peaks_saturdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays12to21 = arima_sundays12
                .keyBy(1)
                .join(peaks_sundays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks12to21;
        addpeaks12to21 = addpeaks_mondays12to21.union(addpeaks_tuesdays12to21).union(addpeaks_wednesdays12to21).union(addpeaks_thursdays12to21).union(addpeaks_fridays12to21).union(addpeaks_saturdays12to21).union(addpeaks_sundays12to21);
        addpeaks12to21.addSink(new InfluxDBSink<>("addpeaks8"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 2 TO 1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays2to1 = arima_mondays2
                .keyBy(1)
                .join(peaks_mondays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays2to1 = arima_tuesdays2
                .keyBy(1)
                .join(peaks_tuesdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays2to1 = arima_wednesdays2
                .keyBy(1)
                .join(peaks_wednesdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays2to1 = arima_thursdays2
                .keyBy(1)
                .join(peaks_thursdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays2to1 = arima_fridays2
                .keyBy(1)
                .join(peaks_fridays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays2to1 = arima_saturdays2
                .keyBy(1)
                .join(peaks_saturdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays2to1 = arima_sundays2
                .keyBy(1)
                .join(peaks_sundays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks2to1;
        addpeaks2to1 = addpeaks_mondays2to1.union(addpeaks_tuesdays2to1).union(addpeaks_wednesdays2to1).union(addpeaks_thursdays2to1).union(addpeaks_fridays2to1).union(addpeaks_saturdays2to1).union(addpeaks_sundays2to1);
        addpeaks2to1.addSink(new InfluxDBSink<>("addpeaks9"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 2 TO 1.2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays2to12 = arima_mondays2
                .keyBy(1)
                .join(peaks_mondays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays2to12 = arima_tuesdays2
                .keyBy(1)
                .join(peaks_tuesdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays2to12 = arima_wednesdays2
                .keyBy(1)
                .join(peaks_wednesdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays2to12 = arima_thursdays2
                .keyBy(1)
                .join(peaks_thursdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays2to12 = arima_fridays2
                .keyBy(1)
                .join(peaks_fridays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays2to12 = arima_saturdays2
                .keyBy(1)
                .join(peaks_saturdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays2to12 = arima_sundays2
                .keyBy(1)
                .join(peaks_sundays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks2to12;
        addpeaks2to12 = addpeaks_mondays2to12.union(addpeaks_tuesdays2to12).union(addpeaks_wednesdays2to12).union(addpeaks_thursdays2to12).union(addpeaks_fridays2to12).union(addpeaks_saturdays2to12).union(addpeaks_sundays2to12);
        addpeaks2to12.addSink(new InfluxDBSink<>("addpeaks10"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 2 TO 2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays2to2 = arima_mondays2
                .keyBy(1)
                .join(peaks_mondays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays2to2 = arima_tuesdays2
                .keyBy(1)
                .join(peaks_tuesdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays2to2 = arima_wednesdays2
                .keyBy(1)
                .join(peaks_wednesdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays2to2 = arima_thursdays2
                .keyBy(1)
                .join(peaks_thursdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays2to2 = arima_fridays2
                .keyBy(1)
                .join(peaks_fridays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays2to2 = arima_saturdays2
                .keyBy(1)
                .join(peaks_saturdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays2to2 = arima_sundays2
                .keyBy(1)
                .join(peaks_sundays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks2to2;
        addpeaks2to2 = addpeaks_mondays2to2.union(addpeaks_tuesdays2to2).union(addpeaks_wednesdays2to2).union(addpeaks_thursdays2to2).union(addpeaks_fridays2to2).union(addpeaks_saturdays2to2).union(addpeaks_sundays2to2);
        addpeaks2to2.addSink(new InfluxDBSink<>("addpeaks11"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 2 TO 2.1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays2to21 = arima_mondays2
                .keyBy(1)
                .join(peaks_mondays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays2to21 = arima_tuesdays2
                .keyBy(1)
                .join(peaks_tuesdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays2to21 = arima_wednesdays2
                .keyBy(1)
                .join(peaks_wednesdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays2to21 = arima_thursdays2
                .keyBy(1)
                .join(peaks_thursdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays2to21 = arima_fridays2
                .keyBy(1)
                .join(peaks_fridays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays2to21 = arima_saturdays2
                .keyBy(1)
                .join(peaks_saturdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays2to21 = arima_sundays2
                .keyBy(1)
                .join(peaks_sundays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks2to21;
        addpeaks2to21 = addpeaks_mondays2to21.union(addpeaks_tuesdays2to21).union(addpeaks_wednesdays2to21).union(addpeaks_thursdays2to21).union(addpeaks_fridays2to21).union(addpeaks_saturdays2to21).union(addpeaks_sundays2to21);
        addpeaks2to21.addSink(new InfluxDBSink<>("addpeaks12"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 2.1 TO 1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays21to1 = arima_mondays21
                .keyBy(1)
                .join(peaks_mondays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays21to1 = arima_tuesdays21
                .keyBy(1)
                .join(peaks_tuesdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays21to1 = arima_wednesdays21
                .keyBy(1)
                .join(peaks_wednesdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays21to1 = arima_thursdays21
                .keyBy(1)
                .join(peaks_thursdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays21to1 = arima_fridays21
                .keyBy(1)
                .join(peaks_fridays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays21to1 = arima_saturdays21
                .keyBy(1)
                .join(peaks_saturdays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays21to1 = arima_sundays21
                .keyBy(1)
                .join(peaks_sundays_seed1)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks21to1;
        addpeaks21to1 = addpeaks_mondays21to1.union(addpeaks_tuesdays21to1).union(addpeaks_wednesdays21to1).union(addpeaks_thursdays21to1).union(addpeaks_fridays21to1).union(addpeaks_saturdays21to1).union(addpeaks_sundays21to1);
        addpeaks21to1.addSink(new InfluxDBSink<>("addpeaks13"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 2.1 TO 1.2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays21to12 = arima_mondays21
                .keyBy(1)
                .join(peaks_mondays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays21to12 = arima_tuesdays21
                .keyBy(1)
                .join(peaks_tuesdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays21to12 = arima_wednesdays21
                .keyBy(1)
                .join(peaks_wednesdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays21to12 = arima_thursdays21
                .keyBy(1)
                .join(peaks_thursdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays21to12 = arima_fridays21
                .keyBy(1)
                .join(peaks_fridays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays21to12 = arima_saturdays21
                .keyBy(1)
                .join(peaks_saturdays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays21to12 = arima_sundays21
                .keyBy(1)
                .join(peaks_sundays_seed12)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks21to12;
        addpeaks21to12 = addpeaks_mondays21to12.union(addpeaks_tuesdays21to12).union(addpeaks_wednesdays21to12).union(addpeaks_thursdays21to12).union(addpeaks_fridays21to12).union(addpeaks_saturdays21to12).union(addpeaks_sundays21to12);
        addpeaks21to12.addSink(new InfluxDBSink<>("addpeaks14"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 2.1 TO 2
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays21to2 = arima_mondays21
                .keyBy(1)
                .join(peaks_mondays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays21to2 = arima_tuesdays21
                .keyBy(1)
                .join(peaks_tuesdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays21to2 = arima_wednesdays21
                .keyBy(1)
                .join(peaks_wednesdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays21to2 = arima_thursdays21
                .keyBy(1)
                .join(peaks_thursdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays21to2 = arima_fridays21
                .keyBy(1)
                .join(peaks_fridays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays21to2 = arima_saturdays21
                .keyBy(1)
                .join(peaks_saturdays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays21to2 = arima_sundays21
                .keyBy(1)
                .join(peaks_sundays_seed2)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks21to2;
        addpeaks21to2 = addpeaks_mondays21to2.union(addpeaks_tuesdays21to2).union(addpeaks_wednesdays21to2).union(addpeaks_thursdays21to2).union(addpeaks_fridays21to2).union(addpeaks_saturdays21to2).union(addpeaks_sundays21to2);
        addpeaks21to2.addSink(new InfluxDBSink<>("addpeaks15"));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // ADD PEAKS 2.1 TO 2.1
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        DataStream<KeyedDataPoint<Double>> addpeaks_mondays21to21 = arima_mondays21
                .keyBy(1)
                .join(peaks_mondays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_tuesdays21to21 = arima_tuesdays21
                .keyBy(1)
                .join(peaks_tuesdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_wednesdays21to21 = arima_wednesdays21
                .keyBy(1)
                .join(peaks_wednesdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_thursdays21to21 = arima_thursdays21
                .keyBy(1)
                .join(peaks_thursdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_fridays21to21 = arima_fridays21
                .keyBy(1)
                .join(peaks_fridays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_saturdays21to21 = arima_saturdays21
                .keyBy(1)
                .join(peaks_saturdays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks_sundays21to21 = arima_sundays21
                .keyBy(1)
                .join(peaks_sundays_seed21)
                .where(new TimestampKeySelectorTuple())
                .equalTo(new TimestampKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(120)))
                .apply(new SubstractValueJoinedStreams());

        DataStream<KeyedDataPoint<Double>> addpeaks21to21;
        addpeaks21to21 = addpeaks_mondays21to21.union(addpeaks_tuesdays21to21).union(addpeaks_wednesdays21to21).union(addpeaks_thursdays21to21).union(addpeaks_fridays21to21).union(addpeaks_saturdays21to21).union(addpeaks_sundays21to21);
        addpeaks21to21.addSink(new InfluxDBSink<>("addpeaks16"));

*/
        env.execute("energyDataGenerator");
    }


        private static class ArimaFunction implements WindowFunction<Tuple3<String, Long, Double>,Tuple3<String, Long, Double>, Tuple, TimeWindow> {

            public void apply (Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Long, Double>> values,
                               Collector<Tuple3<String, Long, Double>> out) throws Exception {

                ArrayList<Tuple3<String, Long, Double>> list = new ArrayList<Tuple3<String, Long, Double>>();

                // Store all the tuples in an arraylist
                for (Tuple3<String, Long, Double> point: values) {

                    list.add(point);

                }

                // implement comparator to order the tuples by its timestamp
                Comparator<Tuple3<String, Long, Double>> comparator = new Comparator<Tuple3<String, Long, Double>>()
                {

                    public int compare(Tuple3<String, Long, Double> tupleA,
                                       Tuple3<String, Long, Double> tupleB)
                    {
                        return tupleA.f1.compareTo(tupleB.f1);
                    }

                };

                // Order tuples by its timestamp
                Collections.sort(list, comparator);

                // Store the ordered tuples in an array to pass it to arima function
                double[] dataArray = new double[list.size()];
                Tuple3 aux;
                for(int i = 0; i < list.size(); i++) {

                    aux = list.get(i);
                    dataArray[i] =  (double)aux.f2;
                }

                // Set ARIMA model parameters.
                int p = 0;
                int d = 0;
                int q = 1;
                int P = 1;
                int D = 1;
                int Q = 0;
                int m = 0;
                int forecastSize = list.size();
                ArimaParams paramsForecast = new ArimaParams(p, d, q, P, D, Q, m);


                ForecastResult forecastResult = Arima.forecast_arima(dataArray, forecastSize, paramsForecast);
                double[] forecastData = forecastResult.getForecast();

                // Collect to the stream the forecasted results
                for(int i = 0; i < list.size(); i++) {

                    aux = list.get(i);
                    aux.f2 = forecastData[i];
                    out.collect (aux);
                }
            }

        }


         private static class AveragePerDay implements WindowFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double>,
                                                                      Tuple3<Integer, Integer, Double>, Tuple, TimeWindow> {

            public void apply (Tuple tuple, TimeWindow window, Iterable<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double>> values,
                                                              Collector<Tuple3<Integer, Integer, Double>> out) throws Exception {
                Double sum = 0.0;
                int count = 0;
                Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double> first_tuple = values.iterator().next();

                for (Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double> point: values) {

                 //System.out.println("TUPLE: " + point.f0 + " " + point.f1 + " "+ point.f2 + " "+ point.f3 + " "+ point.f4 + " "+ point.f5 + " SUM: " + sum + "  COUNT: " + count);

                    sum += point.f8;
                    count++;
                }
                Double average = sum/count;

                //System.out.println("day: " + first_tuple.f3 + ", hour: " + first_tuple.f4 + ", avg: " + average);
                // add a tuple to the arraylist with the average of a certain hour of a certain day of the week
                //ri_day_of_week.add(new Tuple3<Integer, Integer, Double>(first_tuple.f3, first_tuple.f4, average));
                out.collect (new Tuple3<Integer, Integer, Double>(first_tuple.f3, first_tuple.f4, average));
            }
        }


        private static class FilterDay implements FilterFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double>> {

            private int day;

            FilterDay(int dayVal){
                day = dayVal;

            }

            @Override
            public boolean filter(Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double> value) throws Exception {

                return value.f3 == day;
            }
        }

        private static class FilterKey implements FilterFunction<KeyedDataPoint<Double>> {

            private String key;

            FilterKey(String keyVal){
                key = keyVal;

            }

            @Override
            public boolean filter(KeyedDataPoint<Double> value) throws Exception {

                return (value.getKey().equals(key));
            }
        }


        private static class BackToPoint implements MapFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double>, KeyedDataPoint<Double>> {
            @Override
            public KeyedDataPoint<Double> map(Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double> point_tuple) throws Exception {


                return new KeyedDataPoint<Double>(point_tuple.f7, point_tuple.f6,  point_tuple.f8);


            }
        }

    private static class BackToTuple3 implements MapFunction<KeyedDataPoint<Double>, Tuple3<String, Long, Double>> {
        @Override
        public Tuple3<String, Long, Double> map(KeyedDataPoint<Double> point_tuple) throws Exception {


            return new Tuple3<String, Long, Double>(point_tuple.getKey(), point_tuple.getTimeStampMs(),  point_tuple.getValue());


        }
    }


        private static class ExpandDateCalendar implements MapFunction<KeyedDataPoint<Double>, Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double>> {
            @Override
            public Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double> map(KeyedDataPoint<Double> point) throws Exception {



                Calendar calendar = new GregorianCalendar();

                calendar.setTimeInMillis(point.getTimeStampMs());

                int year = calendar.get(Calendar.YEAR);
                int month = calendar.get(Calendar.MONTH);
                int day = calendar.get(Calendar.DAY_OF_MONTH);
                int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
                int hour = calendar.get(Calendar.HOUR_OF_DAY);
                int minute = calendar.get(Calendar.MINUTE);


                return new Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double>(year, month, day, dayOfWeek, hour, minute, point.getTimeStampMs(), point.getKey(), point.getValue());


            }
        }

    private static class DivideValueJoinedStreams implements JoinFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, KeyedDataPoint<Double>> {
        private static final long serialVersionUID = 1L;

        @Override
        public KeyedDataPoint<Double> join(KeyedDataPoint<Double> first, KeyedDataPoint<Double> second) {
            // since the two stream should have the same time stamp, the returned value will have the same timestamp
            //System.out.println("FIRST:" + first.toString()  + "  SECOND: " + second.toString());
            if(second.getValue()!=0.0)
                return new KeyedDataPoint<Double>(first.getKey(), first.getTimeStampMs(),  (first.getValue()/second.getValue()) );
            else
                return new KeyedDataPoint<Double>(first.getKey(), first.getTimeStampMs(),  first.getValue() );
        }
    }

    private static class SubstractValueJoinedStreams implements JoinFunction<Tuple3<String, Long, Double>, KeyedDataPoint<Double>, KeyedDataPoint<Double>> {
        private static final long serialVersionUID = 1L;

        @Override
        public KeyedDataPoint<Double> join(Tuple3<String, Long, Double> first, KeyedDataPoint<Double> second) {
            // since the two stream should have the same time stamp, the returned value will have the same timestamp
            //System.out.println("FIRST:" + first.toString()  + "  SECOND: " + second.toString());

                return new KeyedDataPoint<Double>(second.getKey(), second.getTimeStampMs(),  (first.f2 + second.getValue()) );
        }
    }

        private static class AdjustedTimeSeries implements JoinFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double>, Tuple3<Integer, Integer, Double>, KeyedDataPoint<Double>> {
            private static final long serialVersionUID = 1L;

            @Override
            public KeyedDataPoint<Double> join(Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double> first, Tuple3<Integer, Integer, Double> second) {
                // since the two stream should have the same time stamp, the returned value will have the same timestamp
                //System.out.println("FIRST:" + first.toString()  + "  SECOND: " + second.toString());
                if (first.f4 == second.f1 & second.f2 != 0.0)
                {
                    //System.out.println("key: " + first.f7  + "  , ts: " + first.f6 + ", value: " + second.f2);
                    return new KeyedDataPoint<Double>(first.f7, first.f6, first.f8/second.f2);
                }
                else
                {
                    //System.out.println("IS RETURNING NULL!!!!!!!!!!!!!!!!!!!!");
                    return new KeyedDataPoint<Double>("0",0,0.0);
                }

            }
        }

        private static class Peaks implements JoinFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double>, Tuple3<Integer, Integer, Double>, KeyedDataPoint<Double>> {
            private static final long serialVersionUID = 1L;

            @Override
            public KeyedDataPoint<Double> join(Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double> first, Tuple3<Integer, Integer, Double> second) {
                // since the two stream should have the same time stamp, the returned value will have the same timestamp
                //System.out.println("FIRST:" + first.toString()  + "  SECOND: " + second.toString());
                if (first.f4 == second.f1)
                {
                    //System.out.println("key: " + first.f7  + "  , ts: " + first.f6 + ", value: " + second.f2);
                    return new KeyedDataPoint<Double>(first.f7, first.f6, first.f8 - second.f2);
                }
                else
                {
                    //System.out.println("IS RETURNING NULL!!!!!!!!!!!!!!!!!!!!");
                    return new KeyedDataPoint<Double>("0",0,0.0);
                }

            }
        }

        private static class TimestampKeySelector implements KeySelector<KeyedDataPoint<Double>, Long> {
            @Override
            public Long getKey(KeyedDataPoint<Double> value) {
                return value.getTimeStampMs();
            }
        }

    private static class TimestampKeySelectorTuple implements KeySelector<Tuple3<String, Long, Double>, Long> {
        @Override
        public Long getKey(Tuple3<String, Long, Double> value) {
            return value.f1;
        }
    }

        private static class Selector implements KeySelector<KeyedDataPoint<Double>, String> {
            @Override
            public String getKey(KeyedDataPoint<Double> value) {
                if (value != null) return value.getKey();
                else return null;
            }
        }
        private static class DayOfWeekSelectorTuple9 implements KeySelector<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double>, Integer> {
            @Override
            public Integer getKey(Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Long, String, Double> tuple) {
                return tuple.f3;
            }
        }

        private static class DayOfWeekSelectorTuple3 implements KeySelector<Tuple3<Integer, Integer, Double>, Integer> {
            @Override
            public Integer getKey(Tuple3<Integer, Integer, Double> tuple) {
                return Integer.valueOf(tuple.f0);
            }
        }

        private static class ParseData extends RichMapFunction<String, KeyedDataPoint<Double>> {
            private static final long serialVersionUID = 1L;


            @Override
            public KeyedDataPoint<Double> map(String record) {
                //String rawData = record.substring(1, record.length() - 1);
                String rawData = record;
                String[] data = rawData.split(",");

                // the data look like this...
                // "Col0","Col1","Col2"
                // 7252,19544,0.052    <- Col1: first three digits are the day of the year, origin = "2009-01-01"
                // 7252,19543,0.052             last two digits is the half and hour of the day, a day has 48 halfs: 1:48
                // 7252,19501,0.033             the halfs might not be in order...
                // 7252,19502,0.03

                String dayOfYearStr = data[1].substring(0, 3);
                String halfStr = data[1].substring(3, 5);

                int min = Integer.parseInt(halfStr) * 30; // 30 minutes
                int dayOfYear = Integer.parseInt(dayOfYearStr);

                Calendar calendar = new GregorianCalendar(2009,0,1,0,0,0);
                calendar.add(Calendar.DAY_OF_MONTH, dayOfYear);
                calendar.add(Calendar.MINUTE, min);
                /*System.out.println(calendar.get(Calendar.YEAR)
                                           + "-" + calendar.get(Calendar.MONTH)
                                           + "-" + calendar.get(Calendar.DAY_OF_MONTH)
                                           + " " + calendar.get(Calendar.HOUR_OF_DAY)
                                           + ":" + calendar.get(Calendar.MINUTE)
                                           + ":" + calendar.get(Calendar.SECOND));
                                           */
            long millisTimeStamp = calendar.getTimeInMillis();
            return new KeyedDataPoint<Double>(data[0], millisTimeStamp, Double.valueOf(data[2]));

        }
    }


    private static class ExtractTimestamp extends AscendingTimestampExtractor<KeyedDataPoint<Double>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(KeyedDataPoint<Double> element) {
            return element.getTimeStampMs();
        }
    }



}
