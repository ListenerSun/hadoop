package com.sqt.hadoop.inputformat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-28  11:45
 */
public class WholeRecordReader extends RecordReader<NullWritable,BytesWritable> {

    private FileSplit split ;
    private Configuration configuration;
    private BytesWritable value = new BytesWritable();//定义一个value
    private boolean isProcess = false;//自定义一个状态表示是否在读文件

    //初始化方法
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
         this.split = (FileSplit) split;
        //获取配置信息
        configuration = context.getConfiguration();
    }



    //读取每个文件的方法
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
       if (!isProcess){
           FSDataInputStream fis = null;
           FileSystem fs = null;
           try {
                fs = FileSystem.get(configuration);
               //获取切片路径
               Path path = split.getPath();
               //获取到切片的输入流
               fis = fs.open(path);
               byte[] buf = new byte[(int) split.getLength()];
               //读取数据
               IOUtils.readFully(fis,buf,0,buf.length);
               value.set(buf,0,buf.length);
           } catch (IOException e) {
               e.printStackTrace();
           } finally {
               IOUtils.closeStream(fis);
               IOUtils.closeStream(fs);
           }
           //一次读一个文件，表示该文件读完了
           isProcess = true;
           return true;
       }
        return false;
    }

    //获取当前key，因为设置的NullWritable所以返回NullWritable
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }
    //获取当前value，就是读取的内容
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    //获取当前读取文件状态，
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
/*
public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable>{
    private BytesWritable value = new BytesWritable();
    private FileSplit split;
    private Configuration configuration;
    private boolean isProcess = false;

    // 初始化方法
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 获取切片信息
        this.split = (FileSplit) split;

        // 获取配置信息
        configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!isProcess) {
            FSDataInputStream fis = null;
            try {
                // 按文件整体处理，读取
                FileSystem fs = FileSystem.get(configuration);

                // 获取切片的路径
                Path path = split.getPath();

                // 获取到切片的输入流
                fis = fs.open(path);

                byte[] buf = new byte[(int) split.getLength()];

                // 读取数据
                IOUtils.readFully(fis, buf, 0, buf.length);

                // 设置输出
                value.set(buf, 0, buf.length);

            } finally {
                IOUtils.closeStream(fis);
            }

            isProcess = true;

            return true;
        }

        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {

        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

}
*/
