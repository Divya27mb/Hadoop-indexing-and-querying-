//Author: Divya Patel
//Emil: dpate146@uncc.edu



package org.myorg;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.*;
import java.util.stream.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.log4j.Logger;



public class QueryFullIndex {

	private static final Logger LOG = Logger.getLogger(QueryFullIndex.class);

    public static void main(String[] args) throws Exception {

      //File folder = new File(args[1]);
      //File[] listOfFiles = folder.listFiles();

      HashMap<String, String> map = new HashMap<>();
      BufferedReader reader;
      BufferedReader reader1;
      BufferedReader reader2;
		try {

			reader = new BufferedReader(new FileReader(args[1])); // make a hashmap which contain words as key and all the document id and offset as value
			String line = reader.readLine();
			while (line != null) {
        String[] words = line.split(":");
        map.put(words[0],words[1].trim());
        line = reader.readLine();

			}	reader.close();
    }
	catch (IOException e) {
			e.printStackTrace();
		}

    ArrayList<String> query = new ArrayList<String>();  // make an array which contains all the query words
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


    for (int i = 0; i < query.size(); i++) {
      if (map.containsKey(query.get(i))) {
        bw.newLine();
	bw.write(query.get(i)+ ":"  );
        bw.newLine();
        String temp = map.get(query.get(i));
        String temp_array[]= temp.split("\\+"); // find all the document id and offset for the word by spliting it using +
        //LOG.info(temp_array[0]);
	for(int j=0; j< temp_array.length;j++){
          String t[] = temp_array[j].split("@");// every string split with @ to store the document id and offset indivually
	try {

			reader2 = new BufferedReader(new FileReader(t[0]));
			reader2.skip(Integer.parseInt(t[1]));  //skip offset byte and then readline that line
			bw.write(temp_array[j]+"-> "+reader2.readLine());
			bw.newLine();
                 	reader2.close();
    }
	catch (IOException e) {
			e.printStackTrace();
		}

        }

      } else {
        bw.write(query.get(i)+"->" ); // just writting in output file nothing if word is not found
        bw.newLine();
       }
    }
  bw.close();
 }
}
