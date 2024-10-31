import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.List;
import java.util.ArrayList;

public class Bigram {
    public static class TokenizerMapperBigram extends Mapper<Object, Text, Text, Text> {
        private Text bigram = new Text();
        private final static Text docID = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitvals = value.toString().split("\t", 2);
            String documentID = splitvals[0].trim();
            docID.set(documentID);
            List<String> words = new ArrayList<>();
            words.add("computer science");
            words.add("information retrieval");
            words.add("power politics");
            words.add("los angeles");
            words.add("bruce willis");

            String documentContent = splitvals[1].toLowerCase().replaceAll("[^a-z]", " ");
            documentContent = documentContent.replaceAll("\\s+", " ").trim();
            String[] tokens = documentContent.split(" ");

            for (int i = 0; i < tokens.length - 1; i++) {
                if (!tokens[i].isEmpty() && !tokens[i + 1].isEmpty()) {
                    String b = tokens[i] + " " + tokens[i + 1];
                    for (String w : words) {
                        if (w.equals(b)) {
                            bigram.set(b);
                            context.write(bigram, docID);
                        }
                    }
                }
            }
        }
    }

    public static class IndexReducerBigram extends Reducer<Text, Text, Text, Text> {
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
        Job job = Job.getInstance(conf, "bigram");

        job.setJarByClass(Bigram.class);
        job.setMapperClass(TokenizerMapperBigram.class);
        job.setReducerClass(IndexReducerBigram.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}