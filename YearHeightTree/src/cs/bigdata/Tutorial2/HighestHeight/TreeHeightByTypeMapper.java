package cs.bigdata.Tutorial2.HighestHeight;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TreeHeightByTypeMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	@Override
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
	    {
			String line = valE.toString();
			context.write(new Text(Tree.getGENRE(line)), new FloatWritable(Tree.getHAUTEUR(line)));		
	    }
	

}

