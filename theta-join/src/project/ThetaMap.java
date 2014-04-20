package project;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ThetaMap extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyRow = new Text("");
	private Text keyCol = new Text("");
	private Text value = new Text("");
	private String output = "";
	
	public void map(LongWritable Key, Text Value, Context context) throws IOException, InterruptedException {

		String arr[] = Value.toString().split(",", 21);
		String ymdh = arr[0];
		String userid = arr[1];
		String clicks = arr[3];
		String query = arr[19];
		
		//TODO Matrix Mapping logic comes here. Set keyRow and keyCol.

// Row entries come here		
		
		output = "A" + "," + ymdh.toString() + "," + userid.toString() + "," + clicks.toString() + "," + query.toString(); 
		value.set(output);
		System.out.println(output);
		context.write(keyRow, value);

// Col entries come here
		
		output = "B" + "," + ymdh.toString() + "," + userid.toString() + "," + clicks.toString() + "," + query.toString(); 
		value.set(output);
		System.out.println(output);
		context.write(keyCol, value);
		
	}

} 