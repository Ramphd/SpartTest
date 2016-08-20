package work.liyue.hadoop.test;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by hzliyue1 on 2016/8/20,16:23.
 */
public class HadoopMRWordCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到一行的内容
        String line = value.toString();
        //将行内容切分成单词数组
        String[] words = StringUtils.split(line, ' ');
        for (String word : words) {
            //输出  <单词,1> 这样的key-value对
            context.write(new Text(word), new LongWritable(1L));
        }
    }
}
