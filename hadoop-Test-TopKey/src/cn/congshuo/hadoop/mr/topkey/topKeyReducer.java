package cn.congshuo.hadoop.mr.topkey;


import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jets3t.service.io.TempFile;


public class topKeyReducer extends Reducer<Text,FlowBean , Text, LongWritable>{
	
	private TreeMap<FlowBean,String> treeMap=new TreeMap<>(); //创建一个TreeMap，将你计算好的url和总流量进行排序。注意这里是根据flowBean进行排序的
	
	private long temCount=0;//这个用于查看流量是不是达到了80%
	private double globalCount=0; //存放全部的流量
	@Override
	protected void reduce(Text key1, Iterable<FlowBean> value1, Context context)
			throws IOException, InterruptedException {
		String url=key1.toString();
		//对同一个url的所有的流量进行汇总
		long up_sum=0;
		long down_sum=0;
		for(FlowBean str:value1)
		{
			up_sum+=str.getUp_flow();
			down_sum+=str.getDown_flow();
		}
		FlowBean bean=new FlowBean("", up_sum, down_sum);       //将你计算的总的上行和下行的流量存放在一个FlowBean当中
		globalCount+=bean.getSum_flow();      //这个的目的是得到所有的url的流量的值
		treeMap.put(bean,url); //每当组合成一个bean之后，就将url和bean存放在treeMap当中
		
	}
	
	
	//这里就很棒了。当所有的reduce的方法都调用完毕了。才总体的尽心输出。clean方法是在reduce任务完成只有在调用
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		//将treeMap当中存放的所有的FlowBean和key都提取出来
		Set<Entry<FlowBean, String>> entrySet = treeMap.entrySet();
		
		
		if(temCount/globalCount<0.8)
		{
			//进行for循环，将你的所有的提取到的Entry对象都统一的进行输出
			for(Entry<FlowBean, String> ent:entrySet)
			{
				context.write(new Text(ent.getValue()),new LongWritable(ent.getKey().getSum_flow()) ); //注意这个ent.getKey() 提取出来的是一个FlowBean的变量。你的目的是提取出流量的总值
				temCount+=ent.getKey().getSum_flow();
			}
		}
		else {
			return;
		}
		
	}
}
