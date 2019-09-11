import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSAPI {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.43.128:9000");
        configuration.set("dfs.replication","1");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("/data");
        if(!fileSystem.isDirectory(path)){
            fileSystem.mkdirs(path);
            System.out.println("创建成功");
        }
        fileSystem.copyFromLocalFile(new Path("D:\\test.txt"),path);
//        fileSystem.copyToLocalFile(new Path("/hdfsdemo2/aa.txt"),new Path("E:\\test\\5.txt"));
    }
}
