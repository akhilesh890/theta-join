package project;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.*;

//import org.apache.commons.httpclient.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

public class Driver {

	public static void main(String[] args) throws Exception {

		Configuration conf1 = new Configuration();
		Job job = new Job(conf1, "Document Count");
		job.setJarByClass(Driver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class); 
		job.setMapperClass(ThetaMap.class);
		//job.setCombinerClass(ThetaReduce.class);
		job.setReducerClass(ThetaReduce.class);
		job.setNumReduceTasks(676);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));        
		job.waitForCompletion(true);
		System.out.println("All Tasks Complete. Files Closing.");

	}
}
