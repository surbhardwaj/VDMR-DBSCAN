package dbscan;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool
{
	public int run(String[] args) throws Exception 
	{
		if (args.length < 2) 
		{
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(getConf());
		job.setJobName("Driver");
		job.setJarByClass(Driver.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MY_Mapper.class);
			//job.setCombinerClass(Reducer.class);
		job.setReducerClass(dbscan.myReducer.class);

		job.setInputFormatClass(NLinesInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		// Multiple Input Paths
		MultipleInputs.addInputPath(job, new Path(args[0]), NLinesInputFormat.class, MY_Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), NLinesInputFormat.class, MY_Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), NLinesInputFormat.class, MY_Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[3]), NLinesInputFormat.class, MY_Mapper.class);
		//MultipleInputs.addInputPath(job, new Path(args[4]), NLinesInputFormat.class, MY_Mapper.class);
		//MultipleInputs.addInputPath(job, new Path(args[6]), NLinesInputFormat.class, MY_Mapper.class);
		//MultipleInputs.addInputPath(job, new Path(args[2]), NLinesInputFormat.class, MY_Mapper.class);
		
		//Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[4]);

			/* This line is to accept the input recursively */
		//FileInputFormat.setInputPaths(job, inputFilePath);

		//WholeFileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		
		//job.setNumReduceTasks(1);
		//job.setPartitionerClass(CustomPartitioner.class);

			/*
			 * Delete output filepath if already exists
			 */
			/*FileSystem fs = FileSystem.newInstance(getConf());

			if (fs.exists(outputFilePath)) {
				fs.delete(outputFilePath, true);
			}*/

		return job.waitForCompletion(true) ? 0: 1;
	}

public static void main(String[] args) throws Exception 
{
		Driver driver = new Driver();
		int res = ToolRunner.run(driver, args);
		System.exit(res);
}
}