import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexes {

	public static class MyMapper extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			//folder unused as I was planning to use it for the GUI later
			//String folder = ((FileSplit)context.getInputSplit()).getPath().getParent().getName();
			//get the file name using filesplit
			String file = ((FileSplit)context.getInputSplit()).getPath().getName();
			//remove all escape characters and nuances
			StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^\\x00-\\x7F]", ""), " \t\r\n\r\f\",.:;?![]'*/-()&#");
			//map each token with it's value == filename
			while(itr.hasMoreTokens()){
				String token = itr.nextToken();
				context.write(new Text(token), new Text(file));//+ " folder:/" + folder));
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//hashmap to store the document id and term frequency
			HashMap<String,Integer> documentID = new HashMap<String,Integer>();
			//add term and document to hashmap
			for (Text document_name: values) {
				String k = document_name.toString();
				//if key doesn't exist, put it in the hashmap
				if (!documentID.containsKey(k)){
					documentID.put(k, 1);
					continue;
				}
				//else put increment the term frequency
				documentID.put(k, documentID.get(k) + 1);
			}
			//create a list for each term
			List<String> list = new ArrayList<String>();
			for(String document_name : documentID.keySet()){
				list.add(document_name + "(" + documentID.get(document_name) + ")");
			}
			context.write(key, new Text(list.toString()));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Constructing Inverted Indices");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(InvertedIndexes.class);		
		job.setMapperClass(MyMapper.class);		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//wait for completion
		if(job.waitForCompletion(true)) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}