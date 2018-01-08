package cs.bigdata.Lab2;

/**
 * tf-idf mapper
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
import java.util.StringTokenizer;

public class WordFreqInDocMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	// Overriding of the map method
	@Override
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
	    {
		// Get the name of the file from the inputsplit in the context
			Path filePath = ((FileSplit) context.getInputSplit()).getPath();
        	String fileName = filePath.getName();
        	
        	
			String line = valE.toString();
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens())
			{
				word.set(tokenizer.nextToken()+"@"+fileName);
				context.write(word, one);
			}
	    }

	
}

