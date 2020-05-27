package com.yuanzhss.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 发布微博
 * 互粉
 * 取关
 * 查看微博
 *
 */

public class WeiBo {
    //Hbase的配置对象
    private Configuration conf = HBaseConfiguration.create();

    //创建weibo这个命名空间，3张表
    public static final byte[] NS_WEIBO = Bytes.toBytes("ns_weibi");
    public static final byte[] TABLE_CONTENT = Bytes.toBytes("content");
    public static final byte[] TABLE_RELATION = Bytes.toBytes("relation");
    public static final byte[] TABLE_INBOX = Bytes.toBytes("inbox");

    private void init() throws IOException {
        //创建命名空间
        initNamespace();
        //创建微博内容表
        initTableContent();
        //创建用户关系表
        initTableRelation();
        //创建收件箱
        initTableInbox();
        

    }
    //创建命名空间
    private void initNamespace() throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //创建命名空间描述器
        NamespaceDescriptor weibo = NamespaceDescriptor
                .create("ns_weibo")
                .addConfiguration("creator", "JinJi")
                .addConfiguration("create_time", String.valueOf(System.currentTimeMillis()))
                .build();
        admin.createNamespace(weibo);
        admin.close();
        connection.close();

    }


    /**
     * 表名:ns_weibo:content
     * 列族名:info
     * 列名:content
     * rockey:id_+时间戳
     * value:微博内容(文字内容，图片URL,视频URL,语音URL)
     * Version:1
     * @throws IOException
     */

    private void initTableContent() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //创建表名描述器
        HTableDescriptor contentTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_CONTENT));
        //创建列名描述器
        HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor("info");
        //设置块缓存
        infoColumnDescriptor.setBlockCacheEnabled(true);
        //设置块缓存大小
        infoColumnDescriptor.setBlocksize(2 * 1024 * 1024);
        //设置版本确界
        infoColumnDescriptor.setMinVersions(1);
        infoColumnDescriptor.setMaxVersions(1);

        //将列描述器添加到表描述器中
        contentTableDescriptor.addFamily(infoColumnDescriptor);
        //创建表
        admin.createTable(contentTableDescriptor);
        admin.close();
        connection.close();

    }


    /**
     * 表名:ns_weibo:relation
     * 列族名:attends, fans
     * 列名：用户id
     * rockey:当前操作用户id
     * value:用户id
     * Version: 1
     *
     */
    private void initTableRelation() throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //创建表描述器
        HTableDescriptor relationTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_RELATION));
        //创建列attends描述器
        HColumnDescriptor attendsColumnDescriptor = new HColumnDescriptor("attends");
        attendsColumnDescriptor.setBlockCacheEnabled(true);
        attendsColumnDescriptor.setBlocksize(2 * 1024 * 1024);
        attendsColumnDescriptor.setMinVersions(1);
        attendsColumnDescriptor.setMaxVersions(1);

        //创建列fans描述器
        HColumnDescriptor fansColumnDescriptor = new HColumnDescriptor("fans");
        fansColumnDescriptor.setBlockCacheEnabled(true);
        fansColumnDescriptor.setBlocksize(2 * 1024 * 1024);
        fansColumnDescriptor.setMinVersions(1);
        fansColumnDescriptor.setMaxVersions(1);

        //将列描述器添加到表描述器中
        relationTableDescriptor.addFamily(attendsColumnDescriptor);
        relationTableDescriptor.addFamily(fansColumnDescriptor);

        //创建表
        admin.createTable(relationTableDescriptor);

        admin.close();
        connection.close();

    }

    /**
     * 表名: ns_weibo:inbox
     * 列族名:info
     * 列名:当前用户所关注用户的id
     * rockey: 用户id
     * value:微博的rockey
     * versions: 100
     *
     */
    private void initTableInbox() throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //创建表描述器
        HTableDescriptor inboxTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_INBOX));
        //创建列描述器
        HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor("info");
        infoColumnDescriptor.setBlockCacheEnabled(true);
        infoColumnDescriptor.setBlocksize(2 * 1024 * 1024);
        infoColumnDescriptor.setMinVersions(100);
        infoColumnDescriptor.setMaxVersions(100);

        inboxTableDescriptor.addFamily(infoColumnDescriptor);

        admin.createTable(inboxTableDescriptor);

        admin.close();
        connection.close();



    }

    /**
     * 发布微博
     * a. 向微博内容表中添加刚发布的内容,多了一个微博的rockey
     * b. 向发布微博人的粉丝的收件箱中，添加该微博的rockey
     */

    private void publicContent(String uid, String content) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        //获取表
        Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        //组装rockey 用户id + _ + 时间戳
        long ts = System.currentTimeMillis();
        String rockey = uid + "_" + ts;

        Put contentPut = new Put(Bytes.toBytes(rockey));
        contentPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));

        contentTable.put(contentPut);

        //b 先获取该用户的粉丝id
        Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));

        //先取出所有该用户fans的id放在一个集合里
        ArrayList<byte[]> fans = new ArrayList<>();
        Result result = relationTable.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell: cells) {
            // 取出当前用户fans的所有uid
            fans.add(CellUtil.cloneValue(cell));

        }
        //若没有fans则不操作收件箱
        if (fans.size() <= 0) return;

        //开始操作收件箱

        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
        //封装用于操作粉丝收件箱的Put
        ArrayList<Put> puts = new ArrayList<>();

        for (byte[] fansRockey : fans) {

            Put inboxPut = new Put(fansRockey);

            inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), ts, Bytes.toBytes(rockey));

            puts.add(inboxPut);

        }

        //向收件箱表放置数据
        inboxTable.put(puts);

        inboxTable.close();
        contentTable.close();
        relationTable.close();
        connection.close();


    }

    /**
     * 点击关注
     * a. 当前操作用户往用户关系表中关注列uid_a中添加关注用户uid
     * b. 进入被关注用户的用户关系表中，往被关注列uid_f中添加当前操作用户uid
     * c. 对当前用户的收件箱中添加关注用户的发布微博的rockey
     */

    public void addAttends(String uid, String... attends) throws IOException {

        //参数过滤，如果没有传递关注人的uid，则直接返回
        if (attends == null || attends.length <= 0 || uid == null) return;

        //a
        Connection connection = ConnectionFactory.createConnection(conf);
        Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
        ArrayList<Put> puts = new ArrayList<>();

        Put attendPut = new Put(Bytes.toBytes(uid));

        for (String attend : attends) {

            attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(attend));

            //被关注的人添加粉丝(uid)
            Put fanPut = new Put(Bytes.toBytes(attend));
            fanPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
            puts.add(fanPut);
        }

        //这里需要理解uid是同一个表因此在遍历后才添加
        //被关注人的用户关系表由于遍历attend，因此rockey在变，因此在每次迭代都要添加进去
        //例子put集合(uid, attend1, attend2, attend3, ....)
        puts.add(attendPut);
        relationTable.put(puts);


        //c
        Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        Scan scan = new Scan();
        //用于封装scan扫描出的结果微博的rockey
        ArrayList<byte[]> rowkeys = new ArrayList<>();

        for (String attend : attends) {

            //用rawfilter过滤器扫描出该attend下的微博内容
            RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
            scan.setFilter(filter);
            //通过scan扫描结果
            ResultScanner resultScanner = contentTable.getScanner(scan);
            Iterator<Result> iterator = resultScanner.iterator();
            while (iterator.hasNext()) {

                Result result = iterator.next();
                rowkeys.add(result.getRow());

            }
        }
            //将取出的rockeys放置到当前用户的收件箱中

            //如果关注的人没有一条微博，则返回
        if (rowkeys.size() <=0) return;

        //操作inboxTable
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));

        Put inboxPut = new Put(Bytes.toBytes(uid));

        for (byte[] rowkey: rowkeys) {

            String rockeyString = Bytes.toString(rowkey);
            String attendUid = rockeyString.split("_")[0];
            String attendWeiBoTS = rockeyString.split("_")[1];
            inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attendUid), Long.valueOf(attendWeiBoTS), rowkey);

        }

        inboxTable.put(inboxPut);
        //释放内存
        inboxTable.close();
        contentTable.close();
        relationTable.close();
        connection.close();

    }


    /**
     * 取消关注
     * a. 当前操作用户往用户关系表中关注列uid_a中添加关注用户uid
     * b. 进入被关注用户的用户关系表中，往被关注列uid_f中添加当前操作用户uid
     * c. 对当前用户的收件箱中添加关注用户的发布微博的rockey
     */
    public void deleteAttends(String uid, String ... attends) throws IOException {
        //过滤参数，若不存在attends，则返回
        if (attends == null || attends.length <= 0 || uid == null) return;

        //a
        Connection connection = ConnectionFactory.createConnection(conf);
        //操作当前用户用户关系表
        Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
        //封装put操作
        ArrayList<Delete> deletes = new ArrayList<>();

        Delete attendDelete = new Delete(Bytes.toBytes(uid));

        for (String attend : attends) {
            //当前用户取消关注用户
            attendDelete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend));

            //b. 被关注用户删除关注
            Delete fanDelete = new Delete(Bytes.toBytes(attend));
            fanDelete.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid));
            deletes.add(fanDelete);

        }
        deletes.add(attendDelete);
        relationTable.delete(deletes);

        //c. 使用收件箱表，删除相关attend的微博rowkey

        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));

        Delete delete = new Delete(Bytes.toBytes(uid));

        for (String attend: attends) {

            delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attend));

        }
        inboxTable.delete(delete);

        //释放资源
        relationTable.close();
        inboxTable.close();

        connection.close();


    }

    /**
     * 查看微博内容
     * a.从微博收件箱中获取所有关注人发布的微博的微博rowkey
     * b.根据获取的微博rowkey，前往微博内容表中获取数据
     * c.将取出的数据解码然后封装到Messagr对象中
     */
    public List<Message> getAttendsContent(String uid) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);

        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
        Get rowkeyGet = new Get(Bytes.toBytes(uid));
        rowkeyGet.addFamily(Bytes.toBytes("info"));
        //只取最新的五个版本
        rowkeyGet.setMaxVersions(5);
        Result inboxResult = inboxTable.get(rowkeyGet);

        ArrayList<byte[]> rowkeys = new ArrayList<>();
        Cell[] inboxCells = inboxResult.rawCells();
        for (Cell cell: inboxCells) {

            rowkeys.add(CellUtil.cloneValue(cell));

        }

        //b
        Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        ArrayList<Get> contentGets = new ArrayList<>();
        for (byte[] rowkey : rowkeys) {

            Get contentGet = new Get(rowkey);
            contentGets.add(contentGet);

        }
        ArrayList<Message> messages = new ArrayList<>();
        Result[] contentResults = contentTable.get(contentGets);
        for (Result contentResult: contentResults) {

            Cell[] cells = contentResult.rawCells();
            for (Cell cell : cells) {
                //获取行键
                String rk = Bytes.toString(contentResult.getRow());
                String publishUID = rk.split("_")[0];
                Long publishST = Long.valueOf(rk.split("_")[1]);

                Message msg = new Message();
                msg.setUid(publishUID);
                msg.setTimestamp(publishST);
                msg.setContent(Bytes.toString(CellUtil.cloneValue(cell)));

                messages.add(msg);

            }
        }

        contentTable.close();
        inboxTable.close();
        connection.close();

        return messages;


    }

    //发布微博
    public static void publishWeiBoTest(WeiBo weiBo, String uid, String content) throws IOException {
        weiBo.publicContent(uid, content);
    }

    //关注
    public static void addAttendTest(WeiBo weiBo, String uid, String ... attends) throws IOException {
        weiBo.addAttends(uid, attends);
    }

    //取消关注
    public static void deleteAttendTest(WeiBo weiBo, String uid, String ... attends) throws IOException {
        weiBo.deleteAttends(uid, attends);
    }

    //查看微博内容
    public static void messageTest(WeiBo weiBo, String uid) throws IOException {
        List<Message> attendsContent = weiBo.getAttendsContent(uid);
        System.out.println(attendsContent);
    }

    public static void main(String[] args) throws IOException {
        WeiBo wb = new WeiBo();
//        wb.init();
        publishWeiBoTest(wb, "1002", "测试使用内容111111");
        publishWeiBoTest(wb, "1002", "测试使用内容222222");
        publishWeiBoTest(wb, "1002", "测试使用内容333333");
        publishWeiBoTest(wb, "1003", "测试使用内容444444");

        addAttendTest(wb,"1001", "1002", "1003");

        messageTest(wb, "1001");
    }

}
