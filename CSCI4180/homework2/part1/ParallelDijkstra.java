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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.fs.FileSystem;

public class ParallelDijkstra {
    static enum Iterations{numUpdated};
    
    public static class FinalDijkstraMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		PDNodeWritable node = new PDNodeWritable(value.toString());
		if(node.nodeType != 0){
			IntWritable o_key = new IntWritable(node.nodeID);
			IntWritable o_value = new IntWritable(node.distance);
			context.write(o_key,o_value);
		}
	}
    }
    public static class FinalDijkstraReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
	public void reduce(IntWritable key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
		for(IntWritable val : values){
			context.write(key,val);
		}
	}
    }
    public static class ParallelDijkstraMapper
            extends Mapper<Object, Text, IntWritable, PDNodeWritable>{

	    private int source;
	    private int weighted;
	    
	    protected void setup(Context context)  throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
                this.source = Integer.parseInt(conf.get("source"));
		this.weighted = Integer.parseInt(conf.get("weighted"));
	    }
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		PDNodeWritable node = new PDNodeWritable(value.toString());
		if(node.nodeID == this.source){
			node.distance = 0;
			node.nodeType = 2;
		}
		int distance = node.distance;
		IntWritable output_key = new IntWritable(node.nodeID);
		context.write(output_key,node);
		Iterator<Map.Entry<Integer,Integer>> it = node.adjMap.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<Integer,Integer> entry = it.next();
			IntWritable node_key = new IntWritable(entry.getKey());
			Integer new_distance = 0;
			if(this.weighted==1)
				new_distance = entry.getValue() + distance;
			else if(this.weighted==0)
				new_distance = 1 + distance;
			if(new_distance<0)
				new_distance = Integer.MAX_VALUE;
			PDNodeWritable fake_node = new PDNodeWritable(entry.getKey(),new_distance,0);
			context.write(node_key, fake_node);
		}
            }
	    
    }

    public static class ParallelDijkstraReducer
            extends Reducer<IntWritable,PDNodeWritable,NullWritable,PDNodeWritable> {
            
	    private int source;
	    protected void setup(Context context)  throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
		this.source = Integer.parseInt(conf.get("source"));
            }
            public void reduce(IntWritable key, Iterable<PDNodeWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
		Integer D_min = Integer.MAX_VALUE;
		PDNodeWritable node = new PDNodeWritable(-1,-1,-1);
		Integer new_nodeID = 0;
		for(PDNodeWritable val : values){
			new_nodeID = val.nodeID;
			if(val.isNode == 1){
				node.copy(val);
				//context.write(NullWritable.get(),node);
			}else{
				if(val.distance < D_min)
					D_min = val.distance;
			}
			//context.write(NullWritable.get(),val);
		}
		if(node.nodeID == -1)
			node = new PDNodeWritable(new_nodeID,D_min,1);
		node.distance = D_min;
		if(D_min < Integer.MAX_VALUE){
			if(node.nodeType==0)
				node.nodeType=1;
			else if(node.nodeType==1)
				node.nodeType=2;
		}
		if(node.nodeType==1){
			context.getCounter(Iterations.numUpdated).increment(1);
		}
		if(node.nodeID == source){
			node.distance = 0;
			node.nodeType = 2;
		}
		context.write(NullWritable.get(),node);                
           }
    }

        

    public static void main(String[] args) throws Exception {
	String input_init,output_init;
	input_init = args[0];
	output_init = "tmp_data";
	Configuration conf_init = new Configuration();
        conf_init.set("mapreduce.textoutputformat.separator", " ");
        Job job_init = Job.getInstance(conf_init, "PDPreProcess");
        job_init.setJarByClass(PDPreProcess.class);
        job_init.setMapperClass(PDPreProcess.PDPreProcessMapper.class);
        job_init.setReducerClass(PDPreProcess.PDPreProcessReducer.class);
        job_init.setMapOutputKeyClass(Text.class);
        job_init.setMapOutputValueClass(Text.class);
        job_init.setOutputKeyClass(NullWritable.class);
        job_init.setOutputValueClass(PDNodeWritable.class);
        FileInputFormat.addInputPath(job_init, new Path(input_init));
        FileOutputFormat.setOutputPath(job_init, new Path(output_init));
        job_init.waitForCompletion(true);
	

        Configuration conf = new Configuration();
	Integer source = Integer.parseInt(args[2]);
	Integer number_iteration = Integer.parseInt(args[3]);
	Integer weighted=1;
	//Integer init_isContinue = 0;
	if(args[4].compareTo("weighted") == 0)
		weighted=1;
	else if(args[4].compareTo("nonweighted") == 0)
		weighted=0;
	conf.set("source",source.toString());
	conf.set("number_iteration",number_iteration.toString());
	conf.set("weighted",weighted.toString());
	conf.set("mapreduce.textoutputformat.separator", " ");
	FileSystem hdfs = FileSystem.get(conf);
	Integer exitcode=0;
	Integer numUpdated=1;
	Integer numIterations=1;

	String final_input, final_output;
	while(true){

	//conf.set("continue",init_isContinue.toString());
	String input, output;
	if(numIterations == 1){
		input = output_init;
	}else{
		input = output_init + "_" + (numIterations - 1);
	}
	output = output_init + "_" + numIterations.toString();
	final_input = output;

        Job job = Job.getInstance(conf, "ParallelDijkstra");
	job.setNumReduceTasks(2);
        job.setJarByClass(ParallelDijkstra.class);
        job.setMapperClass(ParallelDijkstraMapper.class);
        job.setReducerClass(ParallelDijkstraReducer.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(PDNodeWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PDNodeWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
	exitcode = job.waitForCompletion(true) ? 0 : 1;

	
	hdfs.delete(new Path(input), true);
	Counters jobCounters = job.getCounters();
	if(number_iteration == 0){
		//Integer isContinue = Integer.parseInt(conf.get("continue"));
		//if(isContinue == 0)
		//	break;
		long counter = job.getCounters().findCounter(Iterations.numUpdated).getValue();
		if(counter == 0 )
			break;
	}else{
		if(number_iteration==numIterations)
			break;
	}
	numIterations+=1;
	}
	final_output = args[1];
	
	Configuration conf_final = new Configuration();
	conf_final.set("mapreduce.textoutputformat.separator", " ");
	Job job_final = Job.getInstance(conf, "FinalDijkstra");
        job_final.setNumReduceTasks(2);
        job_final.setJarByClass(ParallelDijkstra.class);
        job_final.setMapperClass(FinalDijkstraMapper.class);
        job_final.setReducerClass(FinalDijkstraReducer.class);
        job_final.setMapOutputKeyClass(IntWritable.class);
        job_final.setMapOutputValueClass(IntWritable.class);
        job_final.setOutputKeyClass(IntWritable.class);
        job_final.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_final, new Path(final_input));
        FileOutputFormat.setOutputPath(job_final, new Path(final_output));
	exitcode = job_final.waitForCompletion(true) ? 0 : 1;
	hdfs.delete(new Path(final_input), true);
	System.exit(exitcode);
    }
}
