package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SalaryStatisticsReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer count =0;
        double sum= 0;
        String expriResult = "";
        HashMap<String, Integer> map  = new HashMap<String,Integer> ();
        for(Text value:values){
            count++;
            String[] splitInfo = value.toString ().split (",");
            double salary =Double.parseDouble (splitInfo[3].replace("元/月",""));
            sum +=salary;
            String expri =splitInfo[2];
            if(map.containsKey (expri)){
                map.put (expri,map.get(expri)+1);
            }
            else{
                map.put (expri, 1);
            }
        }
        Integer expriCount = 0;
        for(Map.Entry<String,Integer> set:map.entrySet ()){
            expriCount = set.getValue ();
            expriResult+=set.getKey ()+":"+expriCount+","+Math.round (expriCount*1.0*100/count)+"%|";
        }
        expriResult = expriResult.substring (0,expriResult.length ()-1);
        String result ="count:"+count+",avg:"+Math.round (sum/count)+","+expriResult;
        context.write(key,new Text(result));

    }
}
