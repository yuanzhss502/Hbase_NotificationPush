package com.yuanzhss.hbase_syllabus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HbaseDemo {

    public static Configuration conf;

    static{

        conf = HBaseConfiguration.create();

    }

    /*
    判断hbase中表是否存在
     */

    public static boolean isExist(String tableName) throws IOException {

        //老API
//        HBaseAdmin admin = new HBaseAdmin(conf);
        //新API
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();


        return admin.tableExists(TableName.valueOf(tableName));

    }


    /*
    Hbase创建表
     */
    public static void createTable(String tableName, String... columnfamily) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

        for (String column : columnfamily) {

            htd.addFamily(new HColumnDescriptor(column));

        }
        // 创建表
        admin.createTable(htd);

    }


    private static void deleteTable(String tableName) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if (isExist(tableName)) {
            if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
            } else {
                admin.deleteTable(TableName.valueOf(tableName));
                System.out.println("删除成功");

            } } else{

                System.out.println("删除失败");

            }


        }

    //添加数据
    public static void addRow(String tableName, String rockey, String cf, String column, String value) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rockey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        System.out.println("添加成功");

    }

    //删除一行数据
    public static void deleteRow(String tableName, String rockey, String cf) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rockey));
        table.delete(delete);
        System.out.println("删除一行成功");

    }
    
    public static void deleteMultiRow(String tableName, String... rockey) throws IOException {
        
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        ArrayList<Delete> list = new ArrayList<>();
        for (String row : rockey) {

            Delete delete = new Delete(Bytes.toBytes(row));
            list.add(delete);
        }

        table.delete(list);
        System.out.println("删除多行成功");

    }

    // 获得一个数据
    public static void getRow(String tableName, String rockey) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rockey));

        get.addFamily(Bytes.toBytes("info1"));
        get.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"));

        Result result = table.get(get);

        Cell[] cells = result.rawCells();
        for (Cell cell: cells) {

            System.out.println("行键：" + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));

        }


    }

    //扫描数据
    public static void getAllRow(String tableName) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        // 显示所有版本
//        scan.setMaxVersions();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {

            Cell[] cells = result.rawCells();

            for (Cell cell: cells) {

                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族:" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(cell)));

            }

        }


    }


    // 主函数测试
    public static void main(String[] args) throws IOException {
//        System.out.println(isExist("aaa"));
//        createTable("staff", "info1", "info2");
//        deleteTable("student");
//        addRow("staff", "10001", "info1", "name", "jim");
//        addRow("staff", "10002", "info1", "name", "mod");
//        addRow("staff", "10003", "info1", "name", "Harry");
//        deleteRow("staff", "1001", null);
//        deleteMultiRow("staff", "10001", "10002", "10003");
        getRow("staff", "10001");
        

    }


}
