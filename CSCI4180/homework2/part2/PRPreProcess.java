import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.lang.StringBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PRPreProcess {
    

    public static class PRPreProcessMapper
            extends Mapper<Object, Text, Text, Text>{
	    Text minus_one = new Text("-1");
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		String parts[] = value.toString().split(" ",3);	
		context.write(new Text(parts[0]), new Text(parts[1]));
		context.write(new Text(parts[1]), minus_one);
            }
    }

    public static class PRPreProcessReducer
            extends Reducer<Text,Text,NullWritable,PRNodeWritable> {

	    public enum nodeNumberCounter{ number }
            public void reduce(Text key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
		IntWritable NodeID = new IntWritable(Integer.parseInt(key.toString()));
		PRNodeWritable PRNode = new PRNodeWritable(NodeID.get(), new Double("0"), 1);
                for (Text val : values) {
                    String parts[] = val.toString().split(" ",2);
		    int NeighbourID = Integer.parseInt(parts[0]);
		    if(NeighbourID != -1)
		    	PRNode.addNeighbour(NeighbourID);
                }
                context.getCounter(nodeNumberCounter.number).increment(1);
                context.write(NullWritable.get(), PRNode);
                    }
    }

    /*public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
	conf.set("mapreduce.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "PRPreProcess");
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PRPreProcessMapper.class);
        job.setReducerClass(PRPreProcessReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(PRNodeWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }*/
}
