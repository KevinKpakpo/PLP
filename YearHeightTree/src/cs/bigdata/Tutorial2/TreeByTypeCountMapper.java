package cs.bigdata.Tutorial2;


import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class TreeByTypeCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	
	// Overriding of the map method
	@Override
	protected void map(LongWritable keyE, Text line, Context context) throws IOException,InterruptedException
	    {
			String tree = line.toString();
			context.write(new Text(Tree.getGENRE(tree)), one);
			
	    }
	

}



