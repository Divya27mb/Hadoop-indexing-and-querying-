//Author: Divya Patel
//Emil: dpate146@uncc.edu



package org.myorg;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.*;

import org.apache.log4j.Logger;

public  class QueryIndex {

    public static final Logger LOG = Logger.getLogger(QueryIndex.class);
    public static void main(String[] args) throws Exception {

      // File folder = new File(args[1]);
      // File[] listOfFiles = folder.listFiles();
      // LOG.info(listOfFiles );
      HashMap<String, String> map = new HashMap<>();
      BufferedReader reader;
      BufferedReader reader1;
		try {

			reader = new BufferedReader(new FileReader(args[1])); // to make one hash map which has key as word and all the files as value
			String line = reader.readLine();
			while (line != null) {
        String[] words = line.split(":");
        map.put(words[0],words[1].trim());
        line = reader.readLine();

			}	reader.close();
    }catch (IOException e) {
			e.printStackTrace();
		}

    ArrayList<String> query = new ArrayList<String>();// make an array which has all the query word
    try {
      reader1 = new BufferedReader(new FileReader(args[0]));
      String line = reader1.readLine();
      while (line != null) {
        query.add(line.trim());
        line = reader1.readLine();
      }	reader1.close();
    } catch (IOException e) {
      e.printStackTrace();
    }


	File fout = new File(args[2]);
	FileOutputStream fos = new FileOutputStream(fout);
	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

// iterate through the all the query word and if it is in hashmap then write that word and all the files to the output file
    for (int i = 0; i < query.size(); i++) {
      if (map.containsKey(query.get(i))) {
        bw.write(query.get(i)+ ":  " + map.get(query.get(i)));
        bw.newLine();
      } else {
        bw.write(query.get(i)+":  " );
        bw.newLine();
       }
    }
  bw.close();
 }
}
