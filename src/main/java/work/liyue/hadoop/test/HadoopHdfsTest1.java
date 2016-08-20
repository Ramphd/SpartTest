package work.liyue.hadoop.test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

/**
 * Created by hzliyue1 on 2016/8/16,19:30.
 */
public class HadoopHdfsTest1 {

    private FileSystem fileSystem;

    @Before
    public void init() throws IOException {
        Configuration conf = new Configuration(); //加载配置文件
        conf.set("dfs.replication", "1");
        conf.set("fs.defaultFS", "hdfs://211.87.227.209:9000");
        /**
         * 通过FileSystem的get()或newInstance()方法获取文件系统的实例
         */
        fileSystem = FileSystem.get(conf);
    }

    @Test
    public void uploadFile() throws Exception {
        //本地文件
        InputStream is = new FileInputStream(new File("/home/hadoop/baby.jpg"));
        //HDFS上的文件
        FSDataOutputStream os = fileSystem.create(new Path("/baby.jpg"));

        IOUtils.copy(is, os);
    }

    @Test
    public void uploadFileSimple() throws Exception {
        fileSystem.copyFromLocalFile(new Path("/home/hadoop/baby.jpg"), new Path("/baby01.jpg"));
    }

    @Test
    public void downloadFile() throws Exception {
        //HDFS file
        FSDataInputStream is = fileSystem.open(new Path("/123.tt"));
        //local file
        OutputStream os = new FileOutputStream(new File("d:/HOut/123.txt"));

        IOUtils.copy(is, os);
    }

    @Test
    public void downloadFileSimple() throws Exception{
        fileSystem.copyToLocalFile(new Path("/baby.jpg"), new Path("/home/hadoop/baby.new2.jpg"));
    }

    @Test
    public void mkdir() throws Exception{
        //可以一次创建多层目录
        boolean result = fileSystem.mkdirs(new Path("/aa/bb/cc"));
        System.out.println(result ? "success" : "fail"); //echo success
    }

    @Test
    public void rmFileOrDir() throws Exception {
        //第二个参数表示是否递归删除
        boolean result = fileSystem.delete(new Path("/aa"), true);
        System.out.println(result ? "success" : "fail"); //echo success
    }

    @Test
    public void listFiles() throws Exception {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            //file path
            String path = file.getPath().getName();
            //file blocks
            BlockLocation[] blocks = file.getBlockLocations();
            //print info
            System.out.println(path + " has " + blocks.length + " blocks");
        }

        /*
         *  echo:
         *  baby.jpg has 1 blocks
         *  baby01.jpg has 1 blocks
         */
    }


}
