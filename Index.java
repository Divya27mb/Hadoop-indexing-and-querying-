//Author: Divya Patel
//Emil: dpate146@uncc.edu



package org.myorg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

import org.apache.log4j.Logger;

public class Index extends Configured implements Tool{

  private static final Logger LOG = Logger.getLogger(Index.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Index(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "index");

    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(IndexMap.class);
    job.setReducerClass(IndexReducer.class);
    job.setNumReduceTasks(1);                 //So,output will be in sorted manner as show in demo file and no need to append all the files manually to run query code
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }


  public static class IndexMap extends Mapper<LongWritable, Text, Text, Text> {

 private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s+");//deviding  all the word using regex expression
	public void map(LongWritable offset, Text lineText, Context context)
			throws java.io.IOException, InterruptedException {

    FileSplit fileSplit = (FileSplit)context.getInputSplit();
    String DocumentName = fileSplit.getPath().getName(); //reading filename
    String line = lineText.toString(); // reading whole docuemnt



    Text document = new Text();
    document.set(DocumentName);// storing document name as text object

    Text currentWord = new Text();
    for (String word : WORD_BOUNDARY.split(line)) {
      if (word.isEmpty()) {
          continue;  // if word is empty continue
      }
          currentWord = new Text(word); //otherwise add the word and and document id
          context.write(currentWord,document);
      }
	}
}

public static class IndexReducer extends
Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws java.io.IOException, InterruptedException {

    Set<String> wordSet = new TreeSet<>(); // use this datastructure so it will sort and remove dubplicates

    for (Text name : values) {
        wordSet.add(name.toString());
      }

    StringBuilder documentList = new StringBuilder(); //for making one whole string which contains all the file name which has that word

    for(String name: wordSet){
      documentList.append(name);
      documentList.append(",");
    }

    Text finalkey = new Text(key + ": ");
    Text dl = new Text(documentList.toString());
		context.write(finalkey, dl); // store word and all the document id 
	}
}
}
