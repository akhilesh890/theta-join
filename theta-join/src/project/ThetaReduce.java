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

public class ThetaReduce extends Reducer<Text, Text, Text, Text> {
	ArrayList<String> tupleA = new ArrayList<String>();
	ArrayList<String> tupleB = new ArrayList<String>();
	Text output = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		key.clear();
		for (Text val : values) {			
			String arr[] = val.toString().split(",");	
			if (arr[0] == "A"){
				tupleA.add(val.toString());
			}
			if (arr[1] == "B"){
				tupleB.add(val.toString());
		}
		for (String a : tupleA){
			
			String arrayA[] = a.toString().split(",");	
			String setA = arr[0];
			String ymdhA = arr[1];
			int secondsA = Integer.parseInt(ymdhA.substring(ymdhA.length()-2, ymdhA.length())); 
			String useridA = arr[2];
			String clicksA = arr[3];
			String queryA = arr[4];
	
			for(String b : tupleB){
				String arrayB[] = b.toString().split(",");	
				String setB = arr[0];
				String ymdhB = arr[1];
				int secondsB = Integer.parseInt(ymdhB.substring(ymdhB.length()-2, ymdhB.length())); 
				String useridB = arr[2];
				String clicksB = arr[3];
				String queryB = arr[4];
				
				if (useridA != useridB && clicksA == "1" && clicksB == "1" && Math.abs(secondsA - secondsB) < 2){
					output.set(ymdhA + "," + queryA + "," + queryB);
					context.write(key, output);
				}
				
			}
			
		}
			
		//TODO Implement the Join function. For each tuple pair, check the conditions and execute the Join.	
		
		//String result = impressions_count + "," + clicks_count + "," + conversions_count; 
//		output.set(result);
		context.write(key, output);
	}
}
}

