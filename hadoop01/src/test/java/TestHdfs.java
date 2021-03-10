import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

    @Before
    public void init() throws IOException {
        //创建客户端对象(指定角色),hadoop是弱校验，不验证用户是否正确
        //FileSystem fs = FileSystem.get(new URI("hdfs://c1:9000"), conf,"atguigu");

        //创建客户端对象(不指定角色，hadoop根目录设置为777)
        fs = FileSystem.get(conf);
    }

    @After
    public void close() throws IOException {
        if(fs != null){
            fs.close();
        }
    }

    /**
     * @author yan
     * @description  在hadoop上创建目录
     * @date 2021/3/10 16:15
     */
    @Test
    public void testMkdir() throws IOException, InterruptedException, URISyntaxException {
        fs.mkdirs(new Path("/eclipse2"));
    }
    /**
     * @author yan
     * @description  上传文件
     * @date 2021/3/10 15:56
     */
    @Test
    public void testUpload() throws IOException {
        fs.copyFromLocalFile(false,true, new Path("D:/学习资料/a.jpg"), new Path("/eclipse"));
    }

    /**
     * @author yan
     * @description  下载文件
     * @date 2021/3/10 16:00
     */
    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(false,new Path("/wcinput"),new Path("D:/"),true);
    }


}
