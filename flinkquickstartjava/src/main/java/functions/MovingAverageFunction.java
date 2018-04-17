package functions;

import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import data.KeyedDataPoint;
///

public class MovingAverageFunction implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, String, TimeWindow> {


    @Override
    public void apply(String arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
        int count = 0;
        double winsum = 0;
        String winKey = arg0;
        
        //Calendar calendar = new GregorianCalendar();

        // this should be the time stamp of the first point in the window
        long first_point_ts = input.iterator().next().getTimeStampMs();
        //calendar.setTimeInMillis(first_point_ts);
        //System.out.println("   FIRST POINT TIME:" + first_point_ts + "  " + calendar.getTime());
        
        // get the sum of the elements in the window
        for (KeyedDataPoint<Double> in: input) {
        	
        	//calendar.setTimeInMillis(in.getTimeStampMs());        	
        	//System.out.println("   POINT TIME:" + in.getTimeStampMs() + "  " + calendar.getTime());
        	
            winsum = winsum + in.getValue();
            count++;           
        }


        Double avg = winsum/(1.0 * count);
        //System.out.println("MovingAverageFunction: winsum=" +  winsum + "  count=" +
        //+ "  avg=" + avg + "  time=" + window.getStart());
        
        //calendar.setTimeInMillis(window.getStart());
        //System.out.println("   ***WINDOW START TIME:" + window.getStart()  + "  " + calendar.getTime());
        
        //calendar.setTimeInMillis(window.getEnd());
        //System.out.println("   ***WINDOW END   TIME:" + window.getEnd()  + "  " + calendar.getTime());

        KeyedDataPoint<Double> windowAvg = new KeyedDataPoint<>(winKey,first_point_ts, avg);

        out.collect(windowAvg);

    }
}

