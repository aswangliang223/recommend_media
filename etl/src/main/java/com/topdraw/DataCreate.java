package com.topdraw;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


/**
 * @Author wangliang
 * @ClassName DataCreate
 * @Description 播放数据生成
 * @Date 2019/5/28
 **/
public class DataCreate {
    public static void main(String[] args) throws IOException {

        int duratrion = 0;
        String currentTime = "";
        String product = "media_shanghai";
        String [] actionStr = {"MediaPlay","MediaSeek","MediaPause","MediaStop"};
        String userId = "";
        String  mediaCode = "";
        String appId = "";

        String path = "/ai/data/input/his_collect_mediaCode_data";
        Configuration conf = new Configuration();
        conf.set("fs.default","hdfs://ns1");
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(path));
        byte[] bytes = new byte[1024];
        int len = -1;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        while((len = fsDataInputStream.read(bytes)) != -1){
            stream.write(bytes,0,len);
        }
        System.out.println(new String(stream.toByteArray()));
        for(int i =0 ;i < 100000;i++){
            String content = "";
        }

    }
}
