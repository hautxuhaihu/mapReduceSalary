package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class CountEveryExpAvgReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int n = 0;
        double sum = 0;
        double max = 0;
        double min = 1000000;//初始化一个较大值
        for(Text value:values){
            n++;
            String[] splitInfo = value.toString ().split (",");
            double salary =Double.parseDouble (splitInfo[3].replace("元/月",""));
            if (salary>max){
                max=salary;
            }
            if(salary<min){
                min = salary;
            }
            sum+=salary;
        }
        String avgSalary = String.valueOf(sum/n);
        String maxSalary = String.valueOf(max);
        String minSalary = String.valueOf(min);
        double salary= new BigDecimal(avgSalary).setScale(2, RoundingMode. HALF_UP ).doubleValue();
        max= new BigDecimal(maxSalary).setScale(2, RoundingMode. HALF_UP ).doubleValue();
        min= new BigDecimal(minSalary).setScale(2, RoundingMode. HALF_UP ).doubleValue();
        context.write(key,new Text(""+salary+",最高薪资："+max+",最低薪资："+min));
    }
}
