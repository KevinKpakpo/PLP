package cs.bigdata.Lab2;

/**
 * tf-idf 2nd reducer
 * @author Kpakpo Akouete
 *
 */

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// To complete according to your problem

public class WordFreqForDocsReducer extends Reducer<Text,Text,Text,Text> {

    // Overriding of the reduce function

    @Override

    protected void reduce(Text cleI, Iterable<Text> values, Context context) throws IOException,InterruptedException

    {
        
    	int sumOfWordsInDocument = 0;
        Map<String, Integer> tempCounter = new HashMap<String, Integer>();
        for (Text val : values) {
            String[] wordCounter = val.toString().split("=");
            tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
            sumOfWordsInDocument += Integer.parseInt(wordCounter[1]);
        }
        
        for (String wordKey : tempCounter.keySet()) {
            context.write(new Text(wordKey + "@" + cleI.toString()), new Text(tempCounter.get(wordKey) + "/"
                    + sumOfWordsInDocument));
        }

    }

}


