package cn.congshuo.hadoop.mr.topkey;

import java.io.IOException;
import java.net.URL;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.filter.KalmanFilter;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo.Bean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;




public class topKeyMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	//定义一个私有成员变量，将上下行的流量封装到FlowBean 当中
	private FlowBean bean=new FlowBean();
	private Text k=new Text();
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line=value.toString();//拿到一行
		//org.apache.commons.lang.StringUtils 导包
		String [] fields=StringUtils.split(line, "\t");
		
		try {
			if(fields.length>32&&StringUtils.isNotEmpty(fields[26])&&fields[26].startsWith("http"))
			{//当取的一行的字符串的大小大于32个字。fields[26]不是一个空字符串；fields[26]这个字符串是以http开头的，说明这个字符串是正确的
				String url=fields[26];
				long up_flow=Long.parseLong(fields[34]); //对上行流量赋值
				long down_flow=Long.parseLong(fields[35]);
				
				bean.set("", up_flow, down_flow);
				k.set(new Text(url));
				
				context.write(k, bean);
			}
			
		} catch (Exception e) {
			System.out.println(e);    //如果数据不正确就将这个错误输出来
		}
		
		
	}
	

}
