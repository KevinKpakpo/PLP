package cs.bigdata.Lab2;

/**
 * tf-idf 2nd mapper in the pipeline
 * @author Kpakpo Akouete, Amine Belhaj, Darnel Hossie
 * Input: (word@docname , n)
 * Output : ( docname , word=n )
 */

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordFreqForDocsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	// Overriding of the map method
	@Override
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
	    {
		String[] wordAndDocCounter = valE.toString().split("\t");
        String[] wordAndDoc = wordAndDocCounter[0].split("@");
        context.write(new Text(wordAndDoc[1]), new Text(wordAndDoc[0] + "=" + wordAndDocCounter[1]));  
	    }

	
}


