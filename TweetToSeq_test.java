package Mahout;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;

public class TweetToSeq_test {
	
    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.println("Arguments: [input csv file] [output sequence file]");
            return;
        }
        String inputFileName = args[0];
        String outputDirName = args[1];
 
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        Writer writer = SequenceFile.createWriter(fs, configuration, new Path(outputDirName),Text.class, Text.class);
 
        int count = 0;
        BufferedReader reader = new BufferedReader(new FileReader(inputFileName));
        Text key = new Text();
        Text value = new Text();
        
        
        while(true) {
            String line = reader.readLine();
            if (line == null) {
            	break;
            }
           
            
            
            
            line = line.replace("\"", "");
            
            String[] tokens = line.split(",", 5);
            if (tokens.length != 5) {
                System.out.println("Skip line: " + line);
                continue;
            }
            
            
            String category = new String();
            
            if(tokens[1].equals("negative"))
            {
            	category = "Negative :-(";
            }else if(tokens[1].equals("positive"))
            {
            	category = "Positive :-)";
            }else{
            	System.out.println("Skip line: " + line);
            	System.out.println(tokens[1]);
            	continue;
            }	
            
            //category = tokens[1];
            
            String id = tokens[2];
            String message = tokens[4];
            key.set("/" + category + "/" + id);
            value.set(message);
            writer.append(key, value);
            count++;
        }
        writer.close();
        System.out.println("Wrote " + count + " entries.");
    }

}
