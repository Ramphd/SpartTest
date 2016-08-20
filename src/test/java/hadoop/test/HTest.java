package hadoop.test;

import org.junit.Test;
import work.liyue.hadoop.test.HadoopHdfsTest1;

/**
 * Created by hzliyue1 on 2016/8/16,20:00.
 */
public class HTest {


    HadoopHdfsTest1 ht = new HadoopHdfsTest1();
    @Test
   public void test() throws Exception {

        ht.downloadFile();
    }
}
