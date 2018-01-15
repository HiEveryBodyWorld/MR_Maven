import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Test {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		conf.set("fs.defaultFS", "hdfs://192.168.3.31:9000/");
		 
		conf.set("mapreduce.job.jar", "target/MR_Maven-1.0.jar");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "192.168.3.31");
		conf.set("dfs.replication","1");
		//conf.set("yarn.cluster.max-application-priority","0");
		conf.set("yarn.resourcemanager.address", "192.168.3.31:9001");
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("mapreduce.jobhistory.address","192.168.3.31:10020");
		conf.set("mapreduce.jobhistory.webapp.address","192.168.3.31:19888");
		
//		conf.set("mapreduce.application.classpath","{{HADOOP_HOME}}/share/hadoop/mapreduce/*, {{HADOOP_HOME}}/share/hadoop/mapreduce/lib/*");
		Job job = Job.getInstance(conf);
		job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
//		job.setMapperClass(WordMapper.class);
//		job.setReducerClass(WordReducer.class);
//		job.setMapOutputKeyClass(Text.class);//mapper key的输出类型
//		job.setMapOutputValueClass(IntWritable.class);//mapper value的输出类型
//		job.setOutputKeyClass(Text.class);//reducer key的输出类型
//		job.setOutputValueClass(LongWritable.class);//reducer value的输出类型
		
//		FileInputFormat.setInputPaths(job, "F:/bigdata/test/test.txt");
//		FileOutputFormat.setOutputPath(job, new Path("F:/bigdata/test/out/"));
//		job.waitForCompletion(true);
        FileInputFormat.setInputPaths(job, "/wcinput/");
		FileOutputFormat.setOutputPath(job, new Path("/wcoutput1/"));
		job.waitForCompletion(true);
	}
}
