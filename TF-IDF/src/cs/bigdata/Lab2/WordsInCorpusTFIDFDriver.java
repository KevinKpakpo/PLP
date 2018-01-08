package cs.bigdata.Lab2;

/**
 * tf-idf driver 3 : WordsInCorpusTFIDFDriver
 **/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordsInCorpusTFIDFDriver extends Configured implements Tool {
	 
    // where to put the data in hdfs when we're done
    private static final String OUTPUT_PATH = "fichiersLab2/tf-idf/output_job3";
 
    // where to read the data from.
    private static final String INPUT_PATH = "fichiersLab2/tf-idf/output_job2/part-r-00000";
 
    public int run(String[] args) throws Exception {
    	
    	
    	// Last Job : Word in Corpus
    	Job job = Job.getInstance(getConf());
    	//job.setJobName("Words in Corpus, TF-IDF");
    	
    	// On précise les classes MyProgram, Map et Reduce
        job.setJarByClass(WordsInCorpusTFIDFDriver.class);
        job.setMapperClass(WordsInCorpusTFIDFMapper.class);
        job.setReducerClass(WordsInCorpusTFIDFReducer.class);
        
        // Définition des types clé/valeur de notre problème

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Définition des fichiers d'entrée et de sorties 
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        //Getting the number of documents from the original input directory.
        Path inputPath = new Path("fichiersLab2/tf-idf/input/");
        FileSystem fs = inputPath.getFileSystem(getConf());
        FileStatus[] stat = fs.listStatus(inputPath);
 
        //Dirty hack to pass the total number of documents as the job name.
        //The call to context.getConfiguration.get("docsInCorpus") returns null when I tried to pass
        //conf.set("docsInCorpus", String.valueOf(stat.length)) Or even
        //conf.setInt("docsInCorpus", stat.length)
        job.setJobName(String.valueOf(stat.length));
        
        //Suppression du fichier de sortie s'il existe déjà

        if (fs.exists(new Path(OUTPUT_PATH))) {

            fs.delete(new Path(OUTPUT_PATH), true);

        }
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
    	WordsInCorpusTFIDFDriver exempleDriver = new WordsInCorpusTFIDFDriver();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);
    }
        
}
