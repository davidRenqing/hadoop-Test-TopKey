package cn.congshuo.hadoop.mr.ehance;

import java.io.IOException;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class enhanceOutPutformat<K,V> extends FileOutputFormat<K, V>{

	//这个getRecordWriter方法，就是根据你的job读进去一些内容，然后将这些内容按照<key,value>的方式写出去
	//这个函数就是写的接受在reduece阶段接受他的参数。具体这个函数的作用，我现在也还是不太清楚。可能需要进一步的进行研究。
	//现在你实践的已经比较多了。知道在各个阶段会用到什么类。你可以查看这些抽象类的子类。找一个例子去模拟进入这个函数里面，
	//打断点，使用F8 跳转到你这个断点的位置。看每一步变量的值，然后去推测这个函数的作用是什么
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		//在reduce写入数据的时候，会首先调用这个函数的。之后将job当中的数据统一交给那个RecordWriter<K, V>类
		FileSystem fs=FileSystem.get(new Configuration());
		FSDataOutputStream enhancedOs = fs.create(new Path("/liuliang/output/enhancedOs")); //注意这是在hdfs当中的文件系统中创建这个文件夹
		FSDataOutputStream tocrawlOs = fs.create(new Path("/liuliang/output/tocrawlOs"));
		
		
		return new LogEnhanceRecordWriter<K, V>(enhancedOs,tocrawlOs);  //这里要加一个new 关键子才会没有错误
	}

	//RecordWriter<K, V> 是一个抽象类，所以我要重写写一个类，实现这个抽象类。定义写的方法
	public static class LogEnhanceRecordWriter<K,V> extends RecordWriter<K, V>
	{
		private FSDataOutputStream enhancedOs=null;
		private FSDataOutputStream tocrawlOs=null;
		//使用这个显示的构造函数将你的文件的目录的参数传进来
		public LogEnhanceRecordWriter(FSDataOutputStream enhancedOs,FSDataOutputStream tocrawlOs)
		{
			this.enhancedOs=enhancedOs;
			this.tocrawlOs=tocrawlOs;
			
		}
		//之后要重写这两个方法。
		//MarReduce contest写的<key,value> 会作为形参传进这个函数当中来
		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//现在将<key,value>写到不同的文件当中去
			
			if(key.toString().contains(("tocrawl")))//key是以tocrawl结尾的就写到一个文件当中去。否则就写到另外一个文件当中去
			{
				tocrawlOs.write(key.toString().getBytes()); //将你的key写到这个文件当中去
				//我很疑惑，你只是将你的key写入到这个文件当中去，可是你的value值该怎么办呢？
			}
			else
			{
				enhancedOs.write(key.toString().getBytes());
			}
			}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//close就是关闭的含义。你写文件的时候就可以关闭文件流。
			//如果打开数据库就可以关闭数据库
		if(enhancedOs !=null)//如果这个文件流没有被关闭，就调用close方法将他进行关闭
		{
			enhancedOs.close();
		}
		
		if(tocrawlOs !=null)
		{
			tocrawlOs.close();
		}
		}
		}

		
		
	}
