package TVSalesTask1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TvSalesDriver {
	public static void main(String[] args) {
		JobClient my_client = new JobClient();
		JobConf job_conf = new JobConf("Tv Sales Validation");
		
		job_conf.setJobName("SalesValidation");
		
		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(IntWritable.class);
		
		job_conf.setJarByClass(TVSalesTask1.TvSalesDriver.class);
		job_conf.setMapperClass(TVSalesTask1.TvSalesMapper.class);
		job_conf.setNumReduceTasks(0);
		
		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));
		
		my_client.setConf(job_conf);
		try {
			//Run the job
			JobClient.runJob(job_conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
