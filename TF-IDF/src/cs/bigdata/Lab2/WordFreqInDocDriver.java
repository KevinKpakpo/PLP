package cs.bigdata.Lab2;

/**
 * tf-idf 1st job driver in the pipeline
 * WordFreqInDocDriver computes the word frequency in each document of the corpus
 * @author Kpakpo Akouete, Amine Belhaj, Darnel Hossie
 *
 */


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


public class WordFreqInDocDriver extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        if (args.length != 2) {

            System.out.println("Usage: [input] [output]");

            System.exit(-1);

        }


        // Job 1 : Word frequency in doc
        Job job1 = Job.getInstance(getConf());

        job1.setJobName("TF-IDF Computing : Word frequency in doc");


        // On précise les classes MyProgram, Map et Reduce

        job1.setJarByClass(WordFreqInDocDriver.class);

        job1.setMapperClass(WordFreqInDocMapper.class);

        job1.setReducerClass(WordFreqInDocReducer.class);


        // Définition des types clé/valeur de notre problème

        job1.setMapOutputKeyClass(Text.class);

        job1.setMapOutputValueClass(IntWritable.class);


        job1.setOutputKeyClass(Text.class);

        job1.setOutputValueClass(IntWritable.class);

        
        // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)

        FileInputFormat.addInputPath(job1, new Path(args[0]));

        FileOutputFormat.setOutputPath(job1, new Path(args[1]));


        //Suppression du fichier de sortie s'il existe déjà

        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(new Path(args[1]))) {

            fs.delete(new Path(args[1]), true);

        }


        return job1.waitForCompletion(true) ? 0: 1;

    }


    public static void main(String[] args) throws Exception {

    	WordFreqInDocDriver exempleDriver = new WordFreqInDocDriver();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}

