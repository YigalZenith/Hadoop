package com.imooc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.HashMap;

public class HDFSApp {
    /* 第一个程序,后面改造成的Junit
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        URI uri = new URI("hdfs://hadoop000:8020");
        FileSystem hadoop = FileSystem.get(uri, configuration, "hadoop");
        Path path = new Path("/hdfsApi/test");
        boolean result = hadoop.mkdirs(path);
        System.out.print(result);
    }
    */

    public static final String HDFS_PATH = "hdfs://hadoop000:8020";
    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("-----------setUp------------");
        configuration = new Configuration();
        // 设置副本数量
        configuration.set("dfs.replication", "1");
        /**
         * 构造一个访问指定HDFS系统的客户端对象
         * 第一个参数: HDFS的URI
         * 第二个参数: 客户端指定的配置参数
         * 第三个参数: 访问HDFS使用的用户
         */
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    /**
     * 查看文件内容
     */
    @Test
    public void text() throws IOException {
        Path path = new Path("/README.txt");
        FSDataInputStream in = fileSystem.open(path);
        IOUtils.copyBytes(in, System.out, 1024);
    }

    /**
     * 创建文件并写入内容
     */
    @Test
    public void create() throws Exception {
        // FSDataOutputStream out = fileSystem.create(new Path("/hdfsApi/test/a.txt"));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsApi/test/b.txt"));
        out.writeUTF("Hello jsw\n");
        out.flush();
        out.close();
    }

    /**
     * 查看副本数
     * 默认配置在D:\Program Files\apache-maven-3.8.5\repository\org\apache\hadoop\hadoop-hdfs\2.6.0\hadoop-hdfs-2.6.0.jar!\hdfs-default.xml
     */
    @Test
    public void getReplication() {
        String s = configuration.get("dfs.replication");
        System.out.println(s);
    }

    /**
     * 文件重命名
     */
    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("/hdfsApi/test/b.txt");
        Path newPath = new Path("/hdfsApi/test/c.txt");
        boolean result = fileSystem.rename(oldPath, newPath);
        System.out.println(result);
    }

    /**
     * 上传本地文件
     * 目录最后的斜杠带不带都可以
     * 如果上传的文件已存在，默认会覆盖
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        Path src = new Path("D:/README.txt");
        Path dst = new Path("/hdfsApi/test/");
        fileSystem.copyFromLocalFile(src, dst);
    }

    /**
     * 上传本地大文件
     */
    @Test
    public void copyFromLocalBigFile() throws IOException {
        // 创建输入流
        FileInputStream fin = new FileInputStream("G:/downloads/jdk-8u321-windows-x64.exe");
        // 用 BufferedInputStream 封装
        BufferedInputStream in = new BufferedInputStream(fin);

        // 创建输出路径
        Path dst = new Path("/hdfsApi/test/jdk.exe");
        // 创建实现 Progressable 接口的类
        Progressable progressable = new Progressable() {
            // 实现接口中的方法
            @Override
            public void progress() {
                System.out.print(".");
            }
        };
        // 创建输出流
        FSDataOutputStream out = fileSystem.create(dst, progressable);

        // 进行拷贝
        IOUtils.copyBytes(in, out, 4096);
    }

    /**
     * 下载远程文件
     * delSrc: false
     * useRawLocalFileSystem: true
     */
    @Test
    public void copyToLocalFile() throws IOException {
        Path src = new Path("/hdfsApi/test/a.txt");
        Path dst = new Path("G:/");
        fileSystem.copyToLocalFile(false, src, dst, true);
    }

    /**
     * 列出文件夹下所有内容
     */
    @Test
    public void listFiles() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            String directory = fileStatus.isDirectory() ? "目录" : "文件";
            String permission = fileStatus.getPermission().toString();
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(directory + "\t" + permission + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    /**
     * 递归列出文件夹下所有文件
     * @throws IOException
     */
    @Test
    public void listFilesRecursive() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);

        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            String directory = fileStatus.isDirectory() ? "目录" : "文件";
            String permission = fileStatus.getPermission().toString();
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(directory + "\t" + permission + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    /**
     * 查看文件块信息
     */
    @Test
    public void listBlock() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/jdk-8u91-linux-x64.tar.gz"));
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation blockLocation:blockLocations){
            for (String host:blockLocation.getHosts()) {
                System.out.println(host+"\t"+blockLocation.getOffset()+"\t"+blockLocation.getLength());
            }
        }
    }

    /**
     * 删除文件
     */
    @Test
    public void deleteFile() throws IOException{
        boolean result = fileSystem.delete(new Path("/hdfsApi/test/jdk.exe"), false);
        System.out.println(result);
    }

    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("-----------tearDown------------");
    }
}
