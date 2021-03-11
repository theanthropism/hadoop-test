
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;


/**
 * className: TestHdfs
 * description:
 * date: 2021/3/10 15:11
 *
 * @author yan
 */
public class TestHdfs {

    private Configuration conf = new Configuration();

    private FileSystem fs;

    private FileSystem localFs;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        //创建客户端对象(指定角色),hadoop是弱校验，不验证用户是否正确
        fs = FileSystem.get(new URI("hdfs://c1:9000"), conf,"atguigu");

        //创建客户端对象(不指定角色，hadoop根目录设置为777)
        //fs = FileSystem.get(conf);
        //本地文件系统
        localFs = FileSystem.get(new Configuration());
    }

    @After
    public void close() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }

    /**
     * @author yan
     * @description 在hadoop上创建目录
     * @date 2021/3/10 16:15
     */
    @Test
    public void testMkdir() throws IOException, InterruptedException, URISyntaxException {
        fs.mkdirs(new Path("/eclipse2"));
    }

    /**
     * @author yan
     * @description 上传文件
     * @date 2021/3/10 15:56
     */
    @Test
    public void testUpload() throws IOException {
        fs.copyFromLocalFile(false, true, new Path("D:\\软件\\系统\\jdk-8u261-linux-x64.tar.gz"), new Path("/eclipse"));
    }

    /**
     * @author yan
     * @description 自定义上传文件
     * @date 2021/3/10 15:56
     */
    @Test
    public void testMyUpload() throws IOException {
        Path src = new Path("D:/软件/系统/jdk-8u261-linux-x64.tar.gz");
        Path dest = new Path("/jdk-8u261-linux-x64.tar.gz");
        //获取本地输入流
        FSDataInputStream is = localFs.open(src);

        //使用hdfs获取输出流
        FSDataOutputStream os = fs.create(dest,true);

        byte[] buffer = new byte[1024];

        for (int i = 0; i<1024*10; i++){
            is.read(buffer);
            os.write(buffer);
        }

        //关闭流
        IOUtils.closeStream(is);
        IOUtils.closeStream(os);

    }

    /**
     * @author yan
     * @description 下载文件
     * @date 2021/3/10 16:00
     */
    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(false, new Path("/wcinput"), new Path("D:/"), true);
    }

    /**
     * @author yan
     * @description 自定义下载文件
     * @date 2021/3/10 15:56
     */
    @Test
    public void tesFirstBlock() throws IOException {
        Path src = new Path("/jdk-8u261-linux-x64.tar.gz");
        Path dest = new Path("D:/jdk-8u261-linux-x64.tar.gz");
        //使用hdfs获取输出流
        FSDataInputStream is = fs.open(src);

        //获取本地输入流
        FSDataOutputStream os = localFs.create(dest,true);

        byte[] buffer = new byte[1024];
        //定位到流的指定位置
        is.seek(1024*1024);
        for (int i = 0; i<1024*1; i++){
            is.read(buffer);
            os.write(buffer);
        }

        //关闭流
        IOUtils.closeStream(is);
        IOUtils.closeStream(os);

    }

    /**
     * @author yan
     * @description 删除文件  true表示递归删除
     * @date 2021/3/11 10:33
     */
    @Test
    public void testDele() throws Exception {
        fs.delete(new Path("/eclipse/jdk-8u261-linux-x64.tar.gz"), true);
    }

    /**
     * @author yan
     * @description 重命名文件
     * @date 2021/3/11 10:34
     */
    @Test
    public void testRename() throws Exception {
        fs.rename(new Path("/eclipse1"), new Path("/eclipse"));
    }

    /**
     * @author yan
     * @description 判断当前路径是否存在
     * @date 2021/3/11 10:34
     */
    @Test
    public void testIfPathExsits() throws Exception {
        System.out.println(fs.exists(new Path("/eclipse")));
    }

    /**
     * @author yan
     * @description 判断是目录还是文件, FileStatus保存文件的状态
     * @date 2021/3/11 10:34
     */
    @Test
    public void testFileIsDir() throws Exception {
        Path path = new Path("/eclipse");
        //判断是否是路径
        System.out.println(fs.isDirectory(path));
        //判断是否是文件
        System.out.println(fs.isFile(path));

        //上面不建议使用
        FileStatus fileStatus = fs.getFileStatus(path);


        System.out.println(fileStatus.isDirectory());
        System.out.println(fileStatus.isFile());

        //获取数组
        FileStatus[] list = fs.listStatus(path);
        System.out.println(list.toString());
    }
    /**
     * @author yan
     * @description 获取文件快信息
     * @date 2021/3/11 10:34
     */
    @Test
    public void testGetBlockInfomation() throws Exception {

        Path path = new Path("/jdk-8u261-linux-x64.tar.gz");
        RemoteIterator<LocatedFileStatus> status = fs.listLocatedStatus(path);

        while (status.hasNext()) {
            LocatedFileStatus locatedFileStatus = status.next();
            System.out.println(locatedFileStatus.getGroup());

            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            for (BlockLocation bl:blockLocations) {
                System.out.println(bl);
            }
        }
    }
}
