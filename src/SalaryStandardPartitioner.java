import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SalaryStandardPartitioner extends Partitioner<Text, LongWritable> {

    @Override
    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
        if(text.toString().contains("hadoop")){
            return 1;
        }
        return 0;
    }
}
