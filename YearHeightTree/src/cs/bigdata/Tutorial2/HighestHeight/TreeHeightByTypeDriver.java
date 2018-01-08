package cs.bigdata.Tutorial2.HighestHeight;

/**
 * Question 5.3 Problem 3 : The trees of Paris
 * TreeHeightByTypeDriver runs the MapReduce job that computes the height of the highest tree of each type
 * Uses arbres.csv stored on HDFS
 * @author Kpakpo Akouete, Belhaj Amine, Darnel Hossie
 * 
 **/

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TreeHeightByTypeDriver extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        if (args.length != 2) {

            System.out.println("Usage: [input] [output]");

            System.exit(-1);

        }


        // Création d'un job 

        Job job = Job.getInstance(getConf());

        job.setJobName("Tree Highest Height By Type");


        // On précise les classes MyProgram, Map et Reduce

        job.setJarByClass(TreeHeightByTypeDriver.class);

        job.setMapperClass(TreeHeightByTypeMapper.class);
        
        //job.setCombinerClass(TreeHeightByTypeReducer.class);

        job.setReducerClass(TreeHeightByTypeReducer.class);
        
        //job.setNumReduceTasks(3);


        // Définition des types clé/valeur de notre problème

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(FloatWritable.class);


        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(FloatWritable.class);


        // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        //Suppression du fichier de sortie s'il existe déjà

        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(new Path(args[1]))) {

            fs.delete(new Path(args[1]), true);

        }


        return job.waitForCompletion(true) ? 0: 1;

    }


    public static void main(String[] args) throws Exception {

    	TreeHeightByTypeDriver exempleDriver = new TreeHeightByTypeDriver();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}

