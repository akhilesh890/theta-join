package project;
import java.io.IOException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashSet; 
import java.util.Iterator; 
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class ThetaReduce extends Reducer<Text, Text, Text, Text> {
	ArrayList<String> tupleA = new ArrayList<String>();
	ArrayList<String> tupleB = new ArrayList<String>();

	Text output = new Text();

	public long parseDate(String str){
		StringTokenizer st = new StringTokenizer(str," -:");
		GregorianCalendar gc = new GregorianCalendar(Integer.parseInt(st.nextToken()),
				Integer.parseInt(st.nextToken()),
				Integer.parseInt(st.nextToken()),
				Integer.parseInt(st.nextToken()),
				Integer.parseInt(st.nextToken()),
				Integer.parseInt(st.nextToken()));		
		return gc.getTimeInMillis();
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String outputString = "";
		tupleA.clear();
		tupleB.clear();
		for (Text val : values) {

			String arr[] = val.toString().split("\\|");	
			if (arr.length != 5) continue;
			if (arr[0].equals("A")){
				tupleA.add(val.toString());
			}
			if (arr[0].equals("B")){
				tupleB.add(val.toString());
			}
		}

		for (int i = 0 ; i < tupleA.size() ; i++){
			String a = tupleA.get(i);
			String arrayA[] = a.toString().split("\\|");	
			String ymdhA = arrayA[1];
			String useridA = arrayA[2];
			String clicksA = arrayA[3];
			String queryA = arrayA[4];
			for(int j = 0 ; j < tupleB.size() ; j++){
				String b = tupleB.get(j);
				String arrayB[] = b.toString().split("\\|"); 
				String ymdhB = arrayB[1];
				String useridB = arrayB[2];
				String clicksB = arrayB[3];				
				String queryB = arrayB[4];

				if (useridA.equals(useridB) == false && clicksA.equals("1") && clicksB.equals("1") && Math.abs(parseDate(ymdhA) - parseDate(ymdhB)) < 2000){
					outputString = ymdhA + " , " + queryA + " , " + queryB;
					output.set(outputString);
					key.clear();
					context.write(key, output);
				}

			}

		}
	}
}


