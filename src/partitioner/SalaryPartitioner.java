package partitioner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SalaryPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text textInput, Text textOutput, int numPartitions) {
        String company = textInput.toString().split(",")[0];
        if(company.contains("CLONE")){
            return 1;
        }
        return 0;
    }
}
