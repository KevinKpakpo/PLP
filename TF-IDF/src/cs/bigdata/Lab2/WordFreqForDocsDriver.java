package cs.bigdata.Lab2;

/**
 * tf-idf 2nd job driver in the pipeline
 * WordFreqForDocsDriver computes the total number of words for each document of the corpus
 * @author Kpakpo Akouete, Amine Belhaj, Darnel Hossie
 *
 */


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordFreqForDocsDriver extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        if (args.length != 2) {

            System.out.println("Usage: [input] [output]");

            System.exit(-1);

        }

        // Job 2 : Total number of words in doc
        Job job2 = Job.getInstance(getConf());

        job2.setJobName("TF-IDF Computing : Total number of words in doc");


        // On précise les classes MyProgram, Map et Reduce

        job2.setJarByClass(WordFreqForDocsDriver.class);

        job2.setMapperClass(WordFreqForDocsMapper.class);

        job2.setReducerClass(WordFreqForDocsReducer.class);


        // Définition des types clé/valeur de notre problème

        job2.setMapOutputKeyClass(Text.class);

        job2.setMapOutputValueClass(Text.class);


        job2.setOutputKeyClass(Text.class);

        job2.setOutputValueClass(Text.class);
        
        // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)

        
        FileInputFormat.addInputPath(job2, new Path(args[0]));

        FileOutputFormat.setOutputPath(job2, new Path(args[1]));


        //Suppression du fichier de sortie s'il existe déjà

        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(new Path(args[1]))) {

            fs.delete(new Path(args[1]), true);

        }


        return job2.waitForCompletion(true) ? 0: 1;

    }


    public static void main(String[] args) throws Exception {

    	WordFreqForDocsDriver exempleDriver = new WordFreqForDocsDriver();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}
