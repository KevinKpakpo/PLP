package cs.bigdata.Lab2;

/**
 * tf-idf reducer
 * @author Kpakpo Akouete
 *
 */

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

// To complete according to your problem

public class WordFreqInDocReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    // Overriding of the reduce function

    @Override

    protected void reduce(Text cleI, Iterable<IntWritable> listevalI, Context context) throws IOException,InterruptedException

    {
        
        int sum = 0;
        
        for (IntWritable val: listevalI) {

            sum += val.get();

        }

        context.write(cleI,new IntWritable(sum));

    }

}

