package cs.bigdata.Tutorial2;

/**
 * Question 5.3 Problem 3 : The trees of Paris
 * TreeByTypeCountReducer implemments the Reduce job for counting number of trees by type
 * @author Kpakpo Akouete, Belhaj Amine, Darnel Hossie
 * 
 **/

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class TreeByTypeCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text typeOfTree, Iterable<IntWritable> counter, Context context) throws IOException,InterruptedException
    {
    	IntWritable numberOfTrees = new IntWritable(0);

        for (IntWritable count: counter) {

        	numberOfTrees.set(numberOfTrees.get()+count.get());

        }

        context.write(typeOfTree,numberOfTrees);

    }

}


