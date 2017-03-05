package cn.congshuo.hadoop.mr.ehance;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//读取原始数据，抽取其中的url，获得该url指向的网页内容的分析结果，追加到原始日志后面
//网站类别，频道类别，主题词，关键词，主演，导演
//如果在规则中查不到，则输出到带爬清单
public class enchanceMap extends Mapper<LongWritable, Text,Text, NullWritable>{
	
	private HashMap<String,String> ruleMap=new HashMap<>();  //定义一个hashMap。将你的数据库当中的内容写到内存当中
	 
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	String line=value.toString(); 
	String [] fileds=StringUtils.split(line, "\t");
	String url=fileds[26];
	
	}
	
	//setup方法是在map任务的初始化的时候调用的。只会调用一次，以后都不会调用了。以后每一次只会调用Map方法
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		//在这里搞一个链接数据库的程序，之后
		//在这个函数中调用所有的链接数据库的函数，将你数据库当中的数据加载到内存当中。这样读取数据的时候会很快的
		DBLoader.dbLoader(ruleMap); //我已经将一个数据库的连接的java文件放在我的src的目录下面了。使用这个函数，会将我的数据库读取到的东西都存放到我这个treeMap当中
	}
	
	//cleanup方法是各个Maptask运行完成之后会调用的方法
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}
}
