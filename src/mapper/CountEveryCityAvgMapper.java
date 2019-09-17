package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountEveryCityAvgMapper extends Mapper<LongWritable, Text,Text, Text> {
    protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
        String line = value.toString();//对一行数据进行拆分
        String[] splitInfo = line.split(",");
        String city = splitInfo[4];
        try{
            context.write(new Text(city),new Text(value));
        }catch (Exception e){
            System.out.println("本行数据有误(可能为空):"+line);
        }
    }
}
