package cs.bigdata.Tutorial2.HighestHeight;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class TreeHeightByTypeReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {

    @Override

    protected void reduce(Text cleI, Iterable<FloatWritable> listevalI, Context context) throws IOException,InterruptedException

    {
    	
    	float maxValue = Float.MIN_VALUE;
    	
    	for (FloatWritable val : listevalI) {
    		maxValue = Math.max(maxValue, val.get());
    	}

        context.write(cleI, new FloatWritable(maxValue));

    }

}
