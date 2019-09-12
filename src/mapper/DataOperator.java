package mapper;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataOperator {

    public static String handleSalary(String salaryInfo){
        /*
         * 本函数是分析薪资的算法，关系整个薪资的正确性
         * @param infoList:这是我们第一题处理后的数据
         * @return：不改变数据类型，将平均薪资加到原字符串末尾，原+“，”+平均薪资
         * */
        String salary="";
        String[] numStr;
        String[] splitInfo =salaryInfo.split(",");
        salary = splitInfo[3];
        if (salary.contains("千") || salary.contains("K") || salary.contains("+")
                || salary.contains("以") || salary.contains("天") || salary.contains("k")) {
            return "";//不正常，程序直接返回空字符串，不继续执行
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
                    return "";
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
                    return "";
                }
                double avgNum = Double.parseDouble(salary);
                if(avgNum<60){
                    salary = String.valueOf(avgNum*10000/12);
                }else if(avgNum>70000){
                    salary = String.valueOf(avgNum/12);
                }
            }
            double  handledSalary= new BigDecimal(salary).setScale(2, RoundingMode. HALF_UP ).doubleValue();
            salaryInfo = splitInfo[0]+","+splitInfo[1]+","+splitInfo[2]+","+handledSalary+"元/月,"+splitInfo[4]+","+splitInfo[5];
        }
        // 正常则返回正常的字符串
        return salaryInfo;
    }

    public static String handleJob(String salaryInfo){
        /*
        规范化岗位列
        @param salaryInfo:一行信息
        @return：将参数中的岗位信息替换掉后返回信息
         */
        String[] splitInfo = salaryInfo.split(",");
        String job = splitInfo[0];
        if(job.contains("大数据")){
            job = "大数据工程师";
        }else if(job.contains("JAVA")){
            job = "JAVA工程师";
        }else if(job.contains("Python")){
            job = "Python工程师";
        }else if (job.contains("数据库")){
            job = "数据库管理员";
        }else if(job.contains("开发")){
            job = "开发工程师";
        }else if(job.contains("数据")){
            job = "数据工程师";
        }else {
            job = "其他工程师";
        }
        salaryInfo = job+","+splitInfo[1]+","+splitInfo[2]+","+splitInfo[3]+","+splitInfo[4]+","+splitInfo[5];
        return salaryInfo;
    }

    public static String handleExperience(String salaryInfo){
        /*
        规范化经验列。
        @param salaryInfo :一行信息
        @return :正常情况下将参数中的经验列修改后再返回（一行信息）。如果数据有问题返回“”。
         */
        String[] splitInfo = salaryInfo.split(",");
        String experience = splitInfo[2];
        double num = 0;
        if (experience.contains("以上")){
            num = Double.parseDouble(findNum(experience));
        }else if (experience.contains("以下")){
            num = Double.parseDouble(findNum(experience));
        }else {
            String[] splitExp = experience.split("-");
            if(splitExp.length == 2){
                String firstStr = experience.split("-")[0];
                String secondStr = experience.split("-")[1];
                double minNum = Double.parseDouble(findNum(firstStr));
                double maxNum = Double.parseDouble(findNum(secondStr));
                num = 0.5*(minNum+maxNum);
            }else {
                return "";//如果出现其他情况，函数返回“”。
            }
        }
        if (num>=0&&num<3){
            splitInfo[2] =  "0-3年";
        }else if(num>=3&&num<5){
            splitInfo[2] =  "3-5年";
        }else if(num>=5&&num<10){
            splitInfo[2] =  "5-10年";
        }else {
            splitInfo[2] =  "10年以上";
        }
        salaryInfo = splitInfo[0]+","+splitInfo[1]+","+splitInfo[2]+","+splitInfo[3]+","+splitInfo[4]+","+splitInfo[5];
        return salaryInfo;
    }

    public static String findNum(String string){
        /*
        从字符串中找数字，数字以String类型返回
         */
        String pattern = "\\d+";
        Pattern r = Pattern.compile(pattern);
        Matcher matcher = r.matcher(string);
        if(matcher.find()){
            return matcher.group();
        }else {
            return "";
        }
    }

    public static String cloneOne(String salaryInfo){
        String[] splitInfo = salaryInfo.split(",");
        splitInfo[0] = "CLONE"+splitInfo[0];
        salaryInfo = splitInfo[0]+","+splitInfo[1]+","+splitInfo[2]+","+splitInfo[3]+","+splitInfo[4]+","+splitInfo[5];
        return salaryInfo;
    }
}
