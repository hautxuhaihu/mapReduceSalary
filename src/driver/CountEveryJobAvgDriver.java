package driver;

import mapper.CountEveryJobAvgMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.GetAvgSalaryReducer;

import java.io.IOException;

public class CountEveryJobAvgDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("/results/result2/part-r-00000");
        Path output = new Path("/results/result7");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.43.128:9000");
        Job job = Job.getInstance(configuration);

        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }
        job.setJarByClass(CountEveryJobAvgDriver.class);
        job.setMapperClass(CountEveryJobAvgMapper.class);
        job.setReducerClass(GetAvgSalaryReducer.class);
//        job.setPartitionerClass(SalaryPartitioner.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job, output);

        if(job.waitForCompletion(true)){
            System.out.println("执行成功");
        }
    }
}
