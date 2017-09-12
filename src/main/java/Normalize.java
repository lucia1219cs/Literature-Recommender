import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //article1:article2 \t relation
            //collect the relationship list for article1

            String line = value.toString().trim();
            String[] article_relation = line.split("\t");

            String article1 = article_relation[0].split(":")[0];
            String article2 = article_relation[0].split(":")[1];

            //key: article1
            //value: article2=relation
            context.write(new Text(article1), new Text(article2 + "=" + article_relation[1]));

        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //key = article1, value=<article2:relation, article2:relation...>
            //normalize each unit of co-occurrence matrix

            //denominator = sum(relation)
            //relation1/denominator relation2/denominator.....

            int denominator = 0;
            Map<String, Integer> article2_relation_map = new HashMap<String, Integer>();

            for(Text value : values) {
                String[] article2_relation = value.toString().trim().split("=");
                int relation = Integer.parseInt(article2_relation[1]);
                article2_relation_map.put(article2_relation[0], relation);

                denominator += relation;
            }

            Iterator iterator = article2_relation_map.entrySet().iterator();

            while(iterator.hasNext()) {
                Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>)iterator.next();

                //outputkey = movie2
                //outpueValue = movie1=(relation/denominator)

                double relative_relation = (double) entry.getValue()/denominator;
                context.write(new Text(entry.getKey()), new Text(key + "=" + relative_relation));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
