package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class GetAvgSalaryReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int n = 0;
        double sum = 0;
        for(Text value:values){
            n++;
            String[] splitInfo = value.toString ().split (",");
            double salary= new BigDecimal(splitInfo[3].replace("元/月","")).setScale(2, RoundingMode. HALF_UP ).doubleValue();
//            double salary =Double.parseDouble (splitInfo[3].replace("元/月",""));
            sum+=salary;
        }
        String avg = String.valueOf(sum/n);
        context.write(key,new Text(avg));
    }
}
