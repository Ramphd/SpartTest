package work.liyue.hadoop.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Created by hzliyue1 on 2016/08/20,17:16.
 * /**
 * 在调用reduce方法之前有个shuffle过程
 * reduce()方法的输入数据来自于shuffle的输出，格式形如：<hello, {1,1,……}>
 */
public class HadoopMRWordCountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //累加器
        long count = 0;
        //统计单词出现次数
        for (LongWritable value : values) {
            count += value.get();
        }
        //输出key表示的单词的统计结果
        context.write(key, new LongWritable(count));
    }
}
