package cs.bigdata.Tutorial2;

/**
 * Question 5.3 Problem 3 : The trees of Paris
 * TreeByTypeCountDriver runs the MapReduce job that counts the number of tree by type
 * Uses arbres.csv stored on HDFS
 * @author Kpakpo Akouete, Belhaj Amine, Darnel Hossie
 * 
 **/


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TreeByTypeCountDriver extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        if (args.length != 2) {

            System.out.println("Usage: [input] [output]");

            System.exit(-1);

        }


        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

        Job job = Job.getInstance(getConf());

        job.setJobName("Tree By Type Count");


        // On précise les classes MyProgram, Map et Reduce

        job.setJarByClass(TreeByTypeCountDriver.class);

        job.setMapperClass(TreeByTypeCountMapper.class);
        
        //job.setCombinerClass(WordCountReducer.class);

        job.setReducerClass(TreeByTypeCountReducer.class);
        
        //job.setNumReduceTasks(3);


        // Définition des types clé/valeur de notre problème

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);


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

    	TreeByTypeCountDriver exempleDriver = new TreeByTypeCountDriver();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}


