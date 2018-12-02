package com.wjy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class AllTest {
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop133:9000"), configuration, "root");

        // 2 创建目录
//            fs.mkdirs(new Path("/daxian/banzhang"));
//            fs.delete(new Path("/daxian"),true);
        fs.copyFromLocalFile(new Path("C:\\Windows\\System32\\drivers\\etc\\hosts"), new Path("/daxian/banzhang"));

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testDeleteDir() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop133:9000"), conf, "root");
        boolean boo = fileSystem.delete(new Path("/daxian/banzhang/SpringCloud学习.md"), true);
        System.out.println(boo);
        fileSystem.close();
    }

    @Test
    public void uploadFile() throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop133:9000"), configuration, "root");
        fileSystem.copyFromLocalFile(new Path("E:\\学习笔记\\StudyNote\\SpringCloud学习.md"), new Path("/daxian/banzhang/"));
        fileSystem.close();
    }

    @Test
    public void downLoadFile() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop133:9000"), configuration, "root");
        fileSystem.copyToLocalFile(new Path("/daxian/banzhang/SpringCloud学习.md"), new Path("C:\\Users\\wjy\\Desktop"));
        fileSystem.close();
    }

    @Test
    public void rename () throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop133:9000"), configuration, "root");
        boolean rename = fileSystem.rename(new Path("/daxian/banzhang/SpringCloud学习.md"), new Path("/daxian/banzhang/SpringCloud.md"));
        System.out.println(rename);
        fileSystem.close();

    }
}
