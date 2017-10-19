import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
* Lab 2 : Pivot table with hadoop
* Adrien Chevrier
* Pablo Tabales
* */

public class PivotMapper {

    /*Mapper takes maps each word with its position in the line*/
    public static class TokenizerMapper
            /*Input key   : line number
            * Input value : 1 line text
            * Output key  : position in line
            * Output value: Word*/
            extends Mapper<Object, Text, IntWritable, Text>{

                /*init outputs*/
        private IntWritable column = new IntWritable();
        private Text word = new Text();
        /*map method*/
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            /*convert string into an array*/
            String[] itr = value.toString().split(",");
            int i = 0;
            /*we pair each word with its line position and write*/
            for (String myword :
                    itr) {
                /*concat line number with word position*/
                word.set(key.toString().concat(":").concat(myword));
                column.set(i++);
                /*write pair*/
                context.write(column, word);
            }

        }
    }

    /*Reducer takes each key-value pair, concatenates all words with the same key and
    * writes them in a row*/
    public static class IntSumReducer
            /*Input key   : column number
            * Input value : 1 word and its line number
            * Output key  : new line number
            * Output value: new lines*/
            extends Reducer<IntWritable, Text,IntWritable,Text> {
        private Text result = new Text();
        /*reduce method*/
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String sum = "";
            /*treeset will be used to sort the words of each line*/
            Set<String> treeSet = new TreeSet<String>();
            ArrayList<String> kept = new ArrayList<String>();
            List<String> convertedValues = new ArrayList<String>();
            /*create array with all the words*/
            for (Text item : values) {
                convertedValues.add(item.toString());
            }
            /*sort all words by their line number*/
            treeSet.addAll(convertedValues);
            /*remove the line numbers of the words*/
            for (String val :
                    treeSet) {
                kept.add(val.substring(val.indexOf(":")+1, val.length()));
            }

            /*concatenate all the words with the same key*/
            for (int i = 0; i < kept.size(); i++) {
                String val = kept.get(i);
                if (!sum.equals("")) {
                    sum = sum.concat(",".concat(val));
                } else {
                    sum = val;
                }

            }
            /*write result to output*/
            result.set(sum);
            context.write(key, result);
        }
    }
/////////////////////////////////MAIN///////////////////////////////////////////////////////////////////////////////////
    public static void main(String[] args) throws Exception {
                /*set job parameters*/
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PivotMapper");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        /*get input and ouput paths*/
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        /*exit when job is finished*/
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}