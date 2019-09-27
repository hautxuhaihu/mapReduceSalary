package driver;

import mapper.SalaryStatisticsMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import partitioner.SalaryPartitioner;
import reducer.SalaryStatisticsReducer;

import java.io.IOException;

public class SalaryStatisticsDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //hadoop的输入输出路径
        Path input = new Path("/results/result1-5/part-r-00000");
        Path output = new Path("/results/result9");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.43.128:9000");
        Job job = Job.getInstance(configuration);

        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }
        job.setJarByClass(SalaryStatisticsDriver.class);
        job.setMapperClass(SalaryStatisticsMapper.class);
        job.setReducerClass(SalaryStatisticsReducer.class);
        job.setPartitionerClass(SalaryPartitioner.class);
        job.setNumReduceTasks(3);

        //设置mapper的输出格式和reducer的输出格式
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
