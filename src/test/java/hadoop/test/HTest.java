package hadoop.test;

import org.junit.Test;
import work.liyue.hadoop.test.HadoopTest1;

/**
 * Created by hzliyue1 on 2016/8/16,20:00.
 */
public class HTest {


    HadoopTest1 ht = new HadoopTest1();
    @Test
   public void test() throws Exception {

        ht.downloadFile();
    }
}
