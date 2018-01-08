package cs.bigdata.Tutorial2;

/**
 * Question 2.8 : Displaying the content of a compact file
 * Isd_history reads the content of isd-history.txt of NOAA stored on HDFS
 * @author Kpakpo Akouete, Belhaj Amine, Darnel Hossie
 * 
 **/

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DisplayIsdHistoryNOAA {
	String USAF;
	String STATION_NAME;
	String CTRY;
	float ELEV;
	
	public static void main(String[] args) throws IOException {
		String hdfsuri = "hdfs://quickstart.cloudera:8020";
		String hdfsFolder = "fichiersLab2";
		String fileName = "isd-history.txt";
		
		final int START_LINE = 9;
		//Open the file
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsuri);
		
		// FileSystem
		FileSystem fs = FileSystem.get(URI.create(hdfsuri),conf);
		
		FSDataInputStream in = fs.open(new Path(hdfsFolder+"/"+fileName));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			
			int count = 0;
			String line;
			while ((line = br.readLine()) !=null){
				if(count > START_LINE){
					System.out.println("STATION_NAME :" + Isd.getSTATION_NAME(line) + " FIPS :" + Isd.getFIPS(line) + " ALTITUDE :" + Isd.getELEV(line));	
                }
				line = br.readLine();
				count++;
			}
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}

	}

}
