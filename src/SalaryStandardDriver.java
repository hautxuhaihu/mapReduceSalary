import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SalaryStandardDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("/data/salaryall.txt");
        Path output = new Path("/results/result2");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.43.128:9000");
        Job job = Job.getInstance(configuration);

        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }
        job.setJarByClass(SalaryStandardDriver.class);
        job.setMapperClass(SalaryStandardMapper.class);
        job.setReducerClass(SalaryStandardReducer.class);
//        job.setPartitionerClass(SalaryStandardPartitioner.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job, output);

        if(job.waitForCompletion(true)){
            System.out.println("执行成功");
        }
    }
}
