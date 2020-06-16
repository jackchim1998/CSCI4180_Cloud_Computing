import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
 
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

public class PRNodeWritable implements Writable {
       public int isNode;
       public int nodeID;
       public Double rank;
       public List<Integer> adjList;
       
       public void write(DataOutput out) throws IOException {
	 out.writeInt(this.isNode);
	 out.writeInt(this.nodeID);
	 out.writeDouble(this.rank);
	 out.writeInt(this.adjList.size());

	 for(int i=0;i<this.adjList.size();i++){
	 	out.writeInt(this.adjList.get(i));
	 }
	 
       }
       
       public void readFields(DataInput in) throws IOException {
	 this.isNode = in.readInt();
         this.nodeID = in.readInt();
	 this.rank = in.readDouble();
	 int number = in.readInt();
	 this.adjList = new ArrayList<Integer>();
	 for(int i=0;i<2*number;i+=2){
		Integer value = in.readInt();
		this.adjList.add(value); 
	 }
	 
       }

	public PRNodeWritable(){

	}

	public static PRNodeWritable read(DataInput in) throws IOException {
         PRNodeWritable w = new PRNodeWritable();
         w.readFields(in);
         return w;
       }
       
       public PRNodeWritable(int nodeID, Double rank, int isNode){
	 this.nodeID = nodeID;
	 this.rank = rank;
	 this.isNode = isNode;
	 this.adjList = new ArrayList<Integer>();
       }

       public PRNodeWritable(String s){
	 this.adjList = new ArrayList<Integer>();
	 String parts[] = s.split("\\s+");
	 this.isNode = Integer.parseInt(parts[0]);
	 this.nodeID = Integer.parseInt(parts[1]);
	 this.rank = Double.parseDouble(parts[2]);
	 int number = Integer.parseInt(parts[3]);
	 for(int i=4;i<(4+number);i++){
		Integer value = Integer.parseInt(parts[i]);
		this.adjList.add(value);
	 }
	 
       }

       public void addNeighbour(int nodeID){
	 this.adjList.add(nodeID);	
       }

       public void copy(PRNodeWritable new_node){
	 this.isNode = new_node.isNode;
	 this.nodeID = new_node.nodeID;
	 this.rank = new_node.rank;
	 this.adjList = new ArrayList<Integer>();
	 for(int i=0;i<new_node.adjList.size();i++){
		this.adjList.add(new_node.adjList.get(i));
	 }
       }

       public String toString(){
	String s ="";
	s+= Integer.toString(isNode) +" " + Integer.toString(nodeID) +" " + Double.toString(rank) +" " + Integer.toString(adjList.size()) +" ";
	for(int i=0;i<this.adjList.size();i++){
                s += this.adjList.get(i).toString() + " ";
         }
	return s;
       }
     }
