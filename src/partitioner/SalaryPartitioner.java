package partitioner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SalaryPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        if (value.toString ().contains ("北京")) {
            return 1;
        }
        if (value.toString ().contains ("上海")) {
            return 2;
        }
        if (key.toString ().contains ("CLONE")) {
            return 3;
        }
        return 0;
    }
}
