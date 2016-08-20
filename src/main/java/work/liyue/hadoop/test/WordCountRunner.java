package work.liyue.hadoop.test;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by hzliyue1 on 2016/08/20,18:23.
 */
public class WordCountRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "cari");
        //实例化一个Job对象
        Configuration conf = new Configuration();
        conf.addResource("hadoopMR.xml");
        Job job = Job.getInstance(conf);
        //设置job作业所做jar包
        job.setJarByClass(WordCountRunner.class);
        //设置Mapper和Reducer类
        job.setMapperClass(HadoopMRWordCountMap.class);
        job.setReducerClass(HadoopMRWordCountReduce.class);
        //设置Mapper类的输出key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置Reducer类的输出key-value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //指定本次作业要处理的原始文件所在路径（注意，是目录）
        FileInputFormat.setInputPaths(job, new Path("/wordCount.txt"));
        //指定本次作业产生的结果输出路径（也是目录）
        FileOutputFormat.setOutputPath(job, new Path("/out/"));
        //提交本次作业，并打印出详细信息
        job.waitForCompletion(true);
    }

    public static class HadoopMRWordCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {

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

    public static class HadoopMRWordCountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
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

}