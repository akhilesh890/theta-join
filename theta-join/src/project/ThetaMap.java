package project;

import java.io.IOException;
import java.util.Random;
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
	static int SQRT_TASKS = 26;
	public void map(LongWritable Key, Text Value, Context context) throws IOException, InterruptedException {

		String arr[] = Value.toString().split(",");
		String ymdh = arr[0];
		String userid = arr[1];
		String clicks = arr[3];
		String query = arr[19];

		if (clicks.equals("1") == false)
			return;

		int[][] random = new int[SQRT_TASKS][SQRT_TASKS];
		int count = 0;		
		for (int i = 0 ; i < SQRT_TASKS ; i++){
			for (int j = 0 ; j < SQRT_TASKS ; j++){
				random[i][j] = ++count;
			}
		}

		Random rand = new Random();
		int rowRandom = rand.nextInt(SQRT_TASKS);
		int colRandom = rand.nextInt(SQRT_TASKS);

		output = "A" + "|" + ymdh.toString() + "|" + userid.toString() + "|" + clicks.toString() + "|" + query.toString(); 		
		value.set(output);
		for (int j = 0; j < SQRT_TASKS ; j++){
			keyRow.set(String.valueOf(random[rowRandom][j]));			
			context.write(keyRow,value);
		}		

		value.clear();

		output = "B" + "|" + ymdh.toString() + "|" + userid.toString() + "|" + clicks.toString() + "|" + query.toString(); 
		value.set(output);
		for (int i = 0; i < SQRT_TASKS ; i++){
			keyCol.set(String.valueOf(random[i][colRandom]));			
			context.write(keyCol,value);
		}

	}

} 
