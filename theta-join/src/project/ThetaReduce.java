package project;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet; 
import java.util.Iterator; 
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * 
 * @author aswinakhilesh
	Input to the reduce function is key, and list of values. Each value is of the form : impression,clicks,conversions 
 */

public class ThetaReduce extends Reducer<Text, Text, Text, Text> {
	ArrayList<String> tupleR = new ArrayList<String>();
	ArrayList<String> tupleS = new ArrayList<String>();
	Text output = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text val : values) {			
			String arr[] = val.toString().split(",");	
			String set = arr[0];
			String ymdh = arr[1];
			String userid = arr[2];
			String clicks = arr[3];
			String query = arr[4];
			if (arr[0] == "R"){
				
			}
			if (arr[1] == "S"){
				
			}
		}       
		String result = impressions_count + "," + clicks_count + "," + conversions_count; 
		output.set(result);
		context.write(key, output);
	}
}

