package partitioner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SalaryPartitioner extends Partitioner<Text, Text> {
    /**
     * 按照地区分区可以将不同地区的结果保存到不同的文件中，分区号必须从0开始
     * @param key mapper传来的数据
     * @param value mapper传来的value
     * @param numPartitions 分区号
     * @return 返回不同地区的分区号
     */
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
