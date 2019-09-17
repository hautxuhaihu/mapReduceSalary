package driver;

import mapper.CountEveryExpAvgMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.CountEveryExpAvgReducer;

import java.io.IOException;

public class CountEveryExpAvgDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("/results/result1-5/part-r-00000");
        Path output = new Path("/results/result10");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.43.128:9000");
        Job job = Job.getInstance(configuration);

        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }
        job.setJarByClass(CountEveryExpAvgDriver.class);
        job.setMapperClass(CountEveryExpAvgMapper.class);
        job.setReducerClass(CountEveryExpAvgReducer.class);
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
