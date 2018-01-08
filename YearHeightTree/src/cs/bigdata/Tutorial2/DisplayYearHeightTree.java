package cs.bigdata.Tutorial2;

/**
 * Question 2.7 : Displaying the content of a CSV file
 * DisplayYearHeightTree reads the content of arbres.csv stored on HDFS
 * @author Kpakpo Akouete, Belhaj Amine, Darnel Hossie
 * 
 **/


import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


public class DisplayYearHeightTree {

	public static void main(String[] args) throws IOException {
		
		String hdfsuri = "hdfs://quickstart.cloudera:8020";
		String hdfsFolder = "fichiersLab2";
		String fileName = "arbres.csv";
		
		//Configuration
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsuri);
		
		// FileSystem
		FileSystem fs = FileSystem.get(URI.create(hdfsuri),conf);
		
		//Path hdfsFolderPath= new Path(hdfsFolder);
		//System.out.print(fs.exists(hdfsFolderPath));
		FSDataInputStream inputStream = fs.open(new Path(hdfsFolder+"/"+fileName));
		
		try{
			
			InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			
			
			// read line by line
			bufferedReader.readLine();
			String line = null;
			
			while ((line = bufferedReader.readLine() )!=null){
				// Process of the current line
				System.out.println(Tree.getInfo(line));
				// go to next line
				line = bufferedReader.readLine();
				}
		}
		finally{
			//close the file
			fs.close();
		}
 
		
		
	}

}
