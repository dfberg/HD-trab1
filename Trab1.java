package org.dfberg;

/* -------------------------------------------------------------------------------------------------
 *
 * Autores: Diego Felipe Berg Mauricio Pardin
 *          
 *
 *
 *Compilação: 
 *javac -Xlint -classpath `yarn classpath` -d trab1/ Trab1.java
 *
 *Criando JAR:
 *jar -cvf trab1.jar -C trab1/ .
 *
 *Execução:
 *hadoop jar trab1.jar org.dfberg.Trab1 <entrada> <saida>

-------------------------------------------------------------------------------------------------*/
        
import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Trab1 {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text autorWord = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String [] line = value.toString().split(":::"); // Campos separados por :::
        for (String a : line[1].split("::")) { // Autores separados por ::
            for (String p: line[2].replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+")) {
                // remove pontuação e números. 
                if (p.length() > 2)
                autorWord.set(a + "," + p); // chave = Autos + Palavra
                context.write(autorWord, one);
            }
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "Trab1");

    job.setJarByClass(Trab1.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}