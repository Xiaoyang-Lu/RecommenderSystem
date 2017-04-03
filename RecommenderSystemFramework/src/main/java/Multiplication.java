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
			//input: movieB \t movieA=relation  //input: movieB(from) \t movieA(to)=pro
			//pass data to reducer
			String line = value.toString().trim();
			String movie_from = line.split("\t")[0];
			String movie_to_relation = line.split("\t")[1];

			context.write(new Text(movie_from), new Text(movie_to_relation));
			//output: movieB \t movieA=relation
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input: user,movie,rating
			//pass data to reducer
			//output: movie \t user:rating
			String user_movie_rating = value.toString().trim();
			String user = user_movie_rating.split(",")[0];
			String movie = user_movie_rating.split(",")[1];
			String rating = user_movie_rating.split(",")[2];
			context.write(new Text(movie), new Text(user + ":" + rating));
			//output : movieB \t user1 : 3
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB(from) value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
			//collect the data for each movie, then do the multiplication

			HashMap<String,Double> relationMap = new HashMap<String, Double>();
			HashMap<String,Double> ratingMap = new HashMap<String, Double>();
			for(Text value: values)
			{
				if(value.toString().contains("="))
				{
					String movie_to = value.toString().split("=")[0];
					Double relation = Double.parseDouble(value.toString().split("=")[1]);
					relationMap.put(movie_to,relation);
				}
				else
				{
					String user = value.toString().split(":")[0];
					Double rating = Double.parseDouble(value.toString().split(":")[1]);
					ratingMap.put(user,rating);
				}
			}

			for(Map.Entry<String,Double> entry : relationMap.entrySet())
			{
				String movie_to = entry.getKey();
				double relation = entry.getValue();

				for(Map.Entry<String,Double> entry1 : ratingMap.entrySet())
				{
					String user = entry.getKey();
					double rating = entry.getValue();

					String outputKey =  user + ":" + movie_to;
					double outputValue = relation * rating;
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
