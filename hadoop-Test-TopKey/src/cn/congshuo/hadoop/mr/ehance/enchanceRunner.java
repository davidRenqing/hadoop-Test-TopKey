package cn.congshuo.hadoop.mr.ehance;

import org.apache.commons.compress.compressors.pack200.Pack200CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class enchanceRunner extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(enchanceRunner.class);
		job.setMapOutputKeyClass(enchanceMap.class);
		
		job.setOutputKeyClass(Text.class);  //设置Map的key输出的格式
		job.setOutputValueClass(NullWritable.class);//设置Map的value的输出格式
		
		job.setOutputFormatClass(enhanceOutPutformat.class);//设置你的输出<key,value>时的格式。你得设置一下这个类，否则人家不会使用的
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new enchanceRunner(), args);
		System.exit(res);
	}
}
