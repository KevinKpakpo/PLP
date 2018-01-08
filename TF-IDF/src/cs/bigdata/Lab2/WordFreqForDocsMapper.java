package cs.bigdata.Lab2;
/**
 * tf-idf 2nd mapper
 * @author Kpakpo Akouete
 *
 */

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

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


