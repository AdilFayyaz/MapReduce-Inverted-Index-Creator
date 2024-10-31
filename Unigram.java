import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Unigram {
   public static class TokenizerMapperUnigram extends Mapper<Object, Text, Text, Text> {
      private Text word = new Text();
      private final static Text docID = new Text();

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         String[] splitvals = value.toString().split("\t", 2);
         String documentID = splitvals[0].trim();
         docID.set(documentID);
         String documentContent = splitvals[1].toLowerCase().replaceAll("[^a-z]", " ");
         documentContent = documentContent.replaceAll("\\s+", " ").trim();
         String[] tokens = documentContent.split(" ");
         for (String token : tokens) {
            if (!token.isEmpty()) {
               word.set(token);
               context.write(word, docID);
            }
         }
      }
   }

   public static class IndexReducerUnigram extends Reducer<Text, Text, Text, Text> {
      public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
         HashMap<String, Integer> map = new HashMap<String, Integer>();

         for (Text val : values) {
            String docV = val.toString().trim();
            map.put(docV, map.getOrDefault(docV, 0) + 1);
         }

         String result = "";
         for (Map.Entry<String, Integer> entry : map.entrySet()) {
            result += entry.getKey() + ":" + Integer.toString(entry.getValue()) + " ";
         }

         context.write(key, new Text(result));
      }
   }

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "unigram");

      job.setJarByClass(Unigram.class);
      job.setMapperClass(TokenizerMapperUnigram.class);

      job.setReducerClass(IndexReducerUnigram.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}