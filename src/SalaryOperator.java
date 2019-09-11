import java.util.*;

public class SalaryOperator {
    //因为相同职位不同企业工资可能不同，所以定义{"公司名"：{"岗位":[企业1薪资，企业2薪资...]}}
    public static HashMap<String,HashMap<String,ArrayList<String>>> changeDataType(List<String> InfoList){
        /*
         * 本函数主要是将list[string1,string2....]转化为高级的数据类型，用来解决复杂问题（4-6）
         * @param InfoList:装着每条字符串的list，每条字符串，是一个职位的信息
         * @return：高级的数据类型{城市：{岗位：[salary1,salary2....]}}
         */
        HashMap<String,HashMap<String, ArrayList<String>>> allSalaryMap = new HashMap<>();
        for(int i=0;i<InfoList.size();i++){
            String infoString = InfoList.get(i);
            String[] splitInfo = infoString.split(",");
            int endIndex = splitInfo.length-1;//定位list的末尾元素
            //检查map里面是否已经有此城市信息
            String city = splitInfo[endIndex-2];//这是城市名，是外围map的key
            String job = splitInfo[0];//这是工作名，是里面的map的key
            if(allSalaryMap.containsKey(city)){
                //检查map里面有没有岗位的信息,if存在，将薪资加进去，如果没有创建新的岗位
                if (allSalaryMap.get(city).containsKey(job)){
                    allSalaryMap.get(city).get(job).add(splitInfo[endIndex]);//把工资加到list中，key：岗位名称
                }else {
                    ArrayList<String> salaryList = new ArrayList<>();
                    salaryList.add(splitInfo[endIndex]);
                    allSalaryMap.get(city).put(job,salaryList);
                }
            }else {
                //如果map里面没有这个城市，将城市加进去，还有对应这条信息的岗位名称和薪资。
                ArrayList<String> salaryList = new ArrayList<>();
                salaryList.add(splitInfo[endIndex]);//将薪资加到list中
                HashMap<String,ArrayList<String>> hashMap = new HashMap<>();
                hashMap.put(job,salaryList);//组成一个新的map<String,ArrayList[]>
                allSalaryMap.put(city,hashMap);//将上一个map加到外围的map中,得到map{String，map{String,ArrayList[]}}
            }
        }
        return allSalaryMap;
    }

    public static  HashMap<String,HashMap<String,ArrayList<String>>> queryPostAvg
            (HashMap<String,HashMap<String,ArrayList<String>>> allSalaryMap){
        /**
         * @param allSalaryMap:这是一个贯穿整个项目的高级数据类型；
         * @return: allSalaryMap,我们将平均成绩仍然放在这个数据结构中，并返回，让整个系统以这个数据机构为一个数据链，
         * 后续操作都只是对这个数据链进行操作。
         */
        for (Map.Entry<String,HashMap<String,ArrayList<String>>> entryCity : allSalaryMap.entrySet()){
            //遍历字典
            HashMap<String,ArrayList<String>> JobMap  = entryCity.getValue();
            for (Map.Entry<String,ArrayList<String>> entryJob : JobMap.entrySet()){
                ArrayList<String> salaryList = entryJob.getValue();
                double postAvg = getAvg(salaryList);
                entryJob.getValue().add(String.valueOf(postAvg));//将平均工资直接加到value的最后一个，value是一个list
            }
        }
        return allSalaryMap;
    }

    public static HashMap<String,String> getCityAvg(HashMap<String,HashMap<String, ArrayList<String>>> allSalaryMap){
        /*
         * 计算一个城市的薪资均值
         * @param allSalaryMap: 这是我们程序的高级数据类型，存放着城市-岗位-薪资等必要数据
         *                    我们可以根据城市找到这个城市的所有岗位，根据岗位得到其薪资
         * @return:{“城市名”：salary}
         */
        HashMap<String, String> hashMap = new HashMap<>();
        for (Map.Entry<String,HashMap<String,ArrayList<String>>> entryCity : allSalaryMap.entrySet()){
            //遍历字典
            double i = 0;
            HashMap<String,ArrayList<String>> JobMap  = entryCity.getValue();
            for (Map.Entry<String,ArrayList<String>> entryJob : JobMap.entrySet()){
                int endIndex = entryJob.getValue().size()-1;
                i=i+Double.parseDouble(entryJob.getValue().get(endIndex));
            }
            hashMap.put(entryCity.getKey(),String.valueOf(i));
        }
        return hashMap;
    }

    public static double getAvg(List<String> list){
        /*
         * 遍历list中的薪资，并计算所有薪资的均值
         * @param list:存放所有薪资的列表
         * @return :返回均值
         */
        int i=0;
        double salary = 0;
        for (String item : list){
            i++;
            String[] splitStr = item.split(",");
            double singleAvgSalary = Double.parseDouble(splitStr[splitStr.length-1]);
            salary = salary+singleAvgSalary;
        }
        salary = salary/i;
        return salary;
    }

    public static String countAvgSalary(String salaryInfo){
        /*
         * 本函数是分析薪资的算法，关系整个薪资的正确性
         * @param infoList:这是我们第一题处理后的数据
         * @return：不改变数据类型，将平均薪资加到原字符串末尾，原+“，”+平均薪资
         * */
        String salary="";
        String[] numStr;
        boolean isNormal = true;
        String[] splitInfo =salaryInfo.split(",");
        salary = splitInfo[3];
        if (salary.contains("千") || salary.contains("K") || salary.contains("+")
                || salary.contains("以") || salary.contains("天") || salary.contains("k")) {
            isNormal = false;
        }
        else{
            if(salary.contains("万/月")){
                salary = salary.replace("万/月","");
                if(salary.contains("-")){
                    numStr = salary.split("-");
                }else {
                    numStr = salary.split("至");
                }
                if(numStr.length==2){
                    double numBegin = Double.parseDouble(numStr[0]);
                    double numAfter = Double.parseDouble(numStr[1]);
                    double numAvg = 0.5*(numBegin+numAfter)*1000;
                    salary = String.valueOf(numAvg);
                }else if (numStr.length==1){
                    salary = String.valueOf(Double.parseDouble(numStr[0])*1000);
                }else {
                   isNormal = false;
                }
            }else{
                salary = salary.replace("元","");
                salary = salary.replace("万","");
                salary = salary.replace("/","");
                salary = salary.replace("年","");
                salary = salary.replace("月","");
                if(salary.contains("-")){
                    numStr = salary.split("-");
                }else {
                    numStr = salary.split("至");
                }
                if(numStr.length==2){
                    double numBegin = Double.parseDouble(numStr[0]);
                    double numAfter = Double.parseDouble(numStr[1]);
                    double numAvg = 0.5*(numBegin+numAfter);
                    salary = String.valueOf(numAvg);
                }else if (numStr.length==1){
                    salary = numStr[0];
                }else {
                   isNormal = false;
                }
                double avgNum = Double.parseDouble(salary);
                if(avgNum<60){
                    salary = String.valueOf(avgNum*10000/12);
                }else if(avgNum>70000){
                    salary = String.valueOf(avgNum/12);
                }
            }
            salaryInfo = splitInfo[0]+","+splitInfo[1]+","+splitInfo[2]+","+salary+","+splitInfo[4]+","+splitInfo[5];
        }
        // 如果正常则返回正常的字符串，不正常返回空字符串
        if (isNormal){
            return salaryInfo;
        }else {
            return "";
        }
    }
}
