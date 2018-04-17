package functions;

import data.KeyedDataPoint;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
///

public class RawIndexFunction implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, String, TimeWindow> {

//calculate th RI (y/CMA(t))
    //calculate PI for each our hour of the day (0-23) for each distinct weekday (monday ... sunday)
    //normalize them
    //
    @Override
    public void apply(String arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
        int count = 0;
        double rawIndex = 0;
        String winKey = arg0;

        // get the sum of the elements in the window
        for (KeyedDataPoint<Double> in: input) {
         //  rawIndex  = //calculate th RI (y/CMA(t))
           // winKey = in.getKey(); // TODO: this just need to be done once ...??? also counting would not be necessary, how to get the size of this window?
        }


        //KeyedDataPoint<Double> windowRawIndex = new KeyedDataPoint<>(rawIndex,window.getEnd(), rawIndex);

        //out.collect(windowRawIndex);

    }
}

