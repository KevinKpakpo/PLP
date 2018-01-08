package cs.bigdata.Lab2;

/**
 * tf-idf 1st reducer in the pipeline
 * Sum counts of words in the document
 * OUTPUT : (word@docname , n)
 * 
 * @author Kpakpo Akouete, Amine Belhaj, Darnel Hossie
 *
 */
 
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

// To complete according to your problem

public class WordFreqInDocReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    // Overriding of the reduce function

    @Override

    protected void reduce(Text word, Iterable<IntWritable> listOfOnes, Context context) throws IOException,InterruptedException

    {
        
        int sum = 0;
        
        for (IntWritable one: listOfOnes) {

            sum += one.get();

        }

        context.write(word,new IntWritable(sum));

    }

}

