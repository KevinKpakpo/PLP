package cs.bigdata.Lab2;

/**
 * tf-idf 3rd mapper in the pipeline
 * @author Kpakpo Akouete, Amine Belhaj, Darnel Hossie
 * Input: (word@docname , n/N)
 * Output : (word , docname=n/N )
 */

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordsInCorpusTFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    /**
     * @param key is the byte offset of the current line in the file;
     * @param value is the line from the file
     *
     *     PRE-CONDITION: perches@callwild \t 1/31778
     *     POST-CONDITION: perches, callwild=1/31778
     */
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordAndCounters = value.toString().split("\t");
        String[] wordAndDoc = wordAndCounters[0].split("@");                 
        context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
    }
}
