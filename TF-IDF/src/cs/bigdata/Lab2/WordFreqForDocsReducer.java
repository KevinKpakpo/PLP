package cs.bigdata.Lab2;

/**
 * tf-idf 2nd reducer in the pipeline
 * Sums frequency of individual n's in same document
 * OUTPUT : (word@docname , n/N)
 * 
 * @author Kpakpo Akouete, Amine Belhaj, Darnel Hossie
 *
 */

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class WordFreqForDocsReducer extends Reducer<Text,Text,Text,Text> {

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


