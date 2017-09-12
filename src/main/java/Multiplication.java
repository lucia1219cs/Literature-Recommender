import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: article2 \t article1=relation

			String line = value.toString().trim();
			String[] article2_relation = line.split("\t");

			context.write(new Text(article2_relation[0]), new Text(article2_relation[1]));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input: author, article, citation

			String[] line = value.toString().trim().split(",");

			context.write(new Text(line[1]), new Text(line[0] + ":" + line[2]));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = article2 value = <article1=relation, article3=relation... author1:citation, author2:citation...>
			//collect the data for each article, then do the multiplication

			//separate <article1 : relation> <author, citation>

			Map<String, Double> relationMap = new HashMap<String, Double>();
			Map<String, Double> citationMap = new HashMap<String, Double>();

			for (Text value:values) {
				if (value.toString().contains("=")) {
					String[] article_relation = value.toString().trim().split("=");

					relationMap.put(article_relation[0], Double.parseDouble((article_relation[1])));
				}
				else if (value.toString().contains(":")) {
					String[] author_citation = value.toString().trim().split(":");
					citationMap.put(author_citation[0], Double.parseDouble(author_citation[1]));

				}
				else {
					return;
				}
			}

			for(Map.Entry<String, Double> entry : relationMap.entrySet()) {
				String article = entry.getKey();
				double relation = entry.getValue();

				for (Map.Entry<String, Double> element : citationMap.entrySet()) {
					String authorId = element.getKey();
					double rating = element.getValue();

					double outputValue = rating * relation;
					String outputKey = authorId + ":" + article;

					context.write(new Text(outputKey), new DoubleWritable(outputValue));

				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
