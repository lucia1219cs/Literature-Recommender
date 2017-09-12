import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class SimilarityMatrix {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//value = authorId \t article1:citation1, article2:citation2...
			String line = value.toString().trim();
			String[] author_articleCitations = line.split("\t");

			//bad input
			if (author_articleCitations.length != 2) {
				return;
			}

			String[]  author_citations = author_articleCitations[1].split(",");

			for (int i = 0; i < author_citations.length; i++) {
				String article1 = author_citations[i].split(":")[0];

				for (int j = 0; j < author_citations.length; j++) {
					String article2 = author_citations[j].split(":")[0];
					String outputKey = article1 + ":" + article2;
                    //key: article1:article2
					//value: 1
					context.write(new Text(outputKey), new IntWritable(1));
				}
			}
		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//key article:article2 value = iterable<1, 1, 1>
			//calculate two articles have been cited by how many times together

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		
		job.setJarByClass(SimilarityMatrix.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
