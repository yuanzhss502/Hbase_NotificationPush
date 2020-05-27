package com.yuanzhss.weibo;

import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Message {
    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    private  String uid;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    private long timestamp;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    private String content;

    @Override
    public String toString() {

        Date date = new Date(timestamp);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return "Message{" +
                "用户id:" + uid + '\n' +
                ", 发布时间：" + sdf.format(date) + '\n' +
                ", 发布内容：'" + content +
                '}';
    }


}
