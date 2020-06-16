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

public class PageRank {
    static enum Iterations{numUpdated};
    
    public static class FinalPageRankMapper
            extends Mapper<Object, Text, IntWritable, DoubleWritable>{
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		PRNodeWritable node = new PRNodeWritable(value.toString());
		IntWritable node_id = new IntWritable(node.nodeID);
		DoubleWritable node_rank = new DoubleWritable(node.rank);
		context.write(node_id, node_rank);
	}
    }
    public static class FinalPageRankReducer
            extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {
	public void reduce(IntWritable key, Iterable<DoubleWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
		for(DoubleWritable val : values){
			context.write(key,val);
		}
	}
    }

    public static class PageRankMapper
            extends Mapper<Object, Text, IntWritable, PRNodeWritable>{

	    
	    private int first;
	    private int number_node;
	    protected void setup(Context context)  throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
                this.first = Integer.parseInt(conf.get("first"));
		this.number_node = Integer.parseInt(conf.get("number_node"));
	    }
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		PRNodeWritable node = new PRNodeWritable(value.toString());
		Double one = new Double("1.0");
		if(first==1)
			node.rank = one/number_node;
		Double rank_out = node.rank/node.adjList.size();
		IntWritable output_key = new IntWritable(node.nodeID);
		context.write(output_key,node);
		
		for(int i=0;i<node.adjList.size();i++){
			IntWritable node_key = new IntWritable(node.adjList.get(i));
			PRNodeWritable fake_node = new PRNodeWritable(node.adjList.get(i),rank_out,0);
			context.write(node_key, fake_node);
		}
            }
	    
    }

    public static class PageRankReducer
            extends Reducer<IntWritable,PRNodeWritable,NullWritable,PRNodeWritable> {
            
	    
	    protected void setup(Context context)  throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
		
            }
            public void reduce(IntWritable key, Iterable<PRNodeWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
		Double sum = 0.0;
		PRNodeWritable node = new PRNodeWritable(-1,0.0,-1);
		Integer new_nodeID = -1;
		for(PRNodeWritable val : values){
			new_nodeID = val.nodeID;
			if(val.isNode == 1){
				node.copy(val);
				//context.write(NullWritable.get(),node);
			}else{
				sum+=val.rank;
			}
			//context.write(NullWritable.get(),val);
		}
		if(node.nodeID == -1)
			node = new PRNodeWritable(new_nodeID,sum,1);
		node.rank = sum;
		
		
		context.write(NullWritable.get(),node);                
           }
    }

        

    public static void main(String[] args) throws Exception {
	Integer exitcode=0;
	String input_init,output_init;
	input_init = args[2];
	output_init = "tmp_data";
	Configuration conf_init = new Configuration();
        conf_init.set("mapreduce.textoutputformat.separator", " ");
        Job job_init = Job.getInstance(conf_init, "PRPreProcess");
        job_init.setJarByClass(PRPreProcess.class);
        job_init.setMapperClass(PRPreProcess.PRPreProcessMapper.class);
        job_init.setReducerClass(PRPreProcess.PRPreProcessReducer.class);
        job_init.setMapOutputKeyClass(Text.class);
        job_init.setMapOutputValueClass(Text.class);
        job_init.setOutputKeyClass(NullWritable.class);
        job_init.setOutputValueClass(PRNodeWritable.class);
        FileInputFormat.addInputPath(job_init, new Path(input_init));
        FileOutputFormat.setOutputPath(job_init, new Path(output_init));
        exitcode = job_init.waitForCompletion(true) ? 0 : 1;
	long nodeNumber = job_init.getCounters().findCounter(PRPreProcess.PRPreProcessReducer.nodeNumberCounter.number).getValue();	

        Configuration conf = new Configuration();
	Double alpha = Double.parseDouble(args[0]);
	Integer number_iteration = Integer.parseInt(args[1]);
	Integer weighted=1;
	//Integer init_isContinue = 0;
	conf.set("number_node",Long.toString(nodeNumber));
	conf.set("alpha",args[0]);
	conf.set("mapreduce.textoutputformat.separator", " ");
	FileSystem hdfs = FileSystem.get(conf);
	Integer numUpdated=1;
	Integer numIterations=1;

	String final_input, final_output;
	while(true){

	//conf.set("continue",init_isContinue.toString());
	String input, output;
	if(numIterations == 1){
		
		conf.set("first","1");
	}else{
		
		conf.set("first","0");
	}
	input = output_init;
	output = output_init + "_1";
	final_input = input;

        Job job = Job.getInstance(conf, "Pagerank");
	job.setNumReduceTasks(2);
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(PRNodeWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
	exitcode = job.waitForCompletion(true) ? 0 : 1;

	
	hdfs.delete(new Path(input), true);

	job = Job.getInstance(conf, "PRAdjust");
        job.setNumReduceTasks(1);
        job.setJarByClass(PRAdjust.class);
        job.setMapperClass(PRAdjust.PRAdjustMapper.class);
        job.setReducerClass(PRAdjust.PRAdjustReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PRNodeWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);
        FileInputFormat.addInputPath(job, new Path(output));
        FileOutputFormat.setOutputPath(job, new Path(input));
        exitcode = job.waitForCompletion(true) ? 0 : 1;

	hdfs.delete(new Path(output), true);

	Counters jobCounters = job.getCounters();
	if(number_iteration==numIterations)
		break;
	
	numIterations+=1;
	}
	final_output = args[3];
		
	Configuration conf_final = new Configuration();
	conf_final.set("mapreduce.textoutputformat.separator", " ");
	Job job_final = Job.getInstance(conf, "FinalDijkstra");
        job_final.setNumReduceTasks(2);
        job_final.setJarByClass(PageRank.class);
        job_final.setMapperClass(FinalPageRankMapper.class);
        job_final.setReducerClass(FinalPageRankReducer.class);
        job_final.setMapOutputKeyClass(IntWritable.class);
        job_final.setMapOutputValueClass(DoubleWritable.class);
        job_final.setOutputKeyClass(IntWritable.class);
        job_final.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job_final, new Path(final_input));
        FileOutputFormat.setOutputPath(job_final, new Path(final_output));
	exitcode = job_final.waitForCompletion(true) ? 0 : 1;
	hdfs.delete(new Path(final_input), true);
	System.exit(exitcode);
    }
}
