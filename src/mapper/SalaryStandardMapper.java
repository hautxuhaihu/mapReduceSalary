package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SalaryStandardMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
        /*
         * 按行读入数据，并在这里进行数据的拆分
         */
        String handledRow = "";
        String[] splitInfo = value.toString().split(",");//对一行数据进行拆分

        if (splitInfo.length == 8) {
            //8列的数据正常
            if (!(splitInfo[3].contains("不限经验") || splitInfo[4].contains("面议"))) {
                //删除第一列和网址列
                List<String> list = Arrays.asList(splitInfo);
                List<String> arrayList = new ArrayList<>(list);
                arrayList.remove(0);
                arrayList.remove(4);
                handledRow = String.join(",", arrayList);
            }
        }
        if(!handledRow.equals("")){
            String line = DataOperator.handleSalary(handledRow);
            //在处理规范化薪资的时候，生成空行，在要判断
            if(!line.equals("")){
                line = DataOperator.handleJob(line);
                line = DataOperator.handleExperience(line);
                if(!line.equals("")){
                    context.write(new Text(line),NullWritable.get());//传给reducer
                }
            }
        }
    }
}
