package com.zheng.hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * hbase java client操作
 * Created by zhenglian on 2017/12/28.
 */
public class SimpleTest {
    private Connection connection;
    private HBaseAdmin admin;
    @Before
    public void init() throws Exception {
        connection = ConnectionFactory.createConnection();
        admin = (HBaseAdmin) connection.getAdmin();
    }
    
    @Test
    public void createTable() throws IOException {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("article"));
        desc.addFamily(new HColumnDescriptor("bref"));
        desc.addFamily(new HColumnDescriptor("content"));
        admin.createTable(desc);
    }
    
    @Test
    public void add() throws IOException {
        Put put = new Put(Bytes.toBytes("wenxue-001"));
        put.add(Bytes.toBytes("bref"), Bytes.toBytes("author"), Bytes.toBytes("张三"));
        Table table = connection.getTable(TableName.valueOf("article"));
        table.put(put);
    }
    
    @Test
    public void batchAdd() throws IOException {
        List<Put> puts = new ArrayList<>();
        Put put = new Put(Bytes.toBytes("wenxue-001"));
        put.add(Bytes.toBytes("bref"), Bytes.toBytes("author"), Bytes.toBytes("lisi"));
        puts.add(put);
        put = new Put(Bytes.toBytes("wenxue-001"));
        put.add(Bytes.toBytes("content"), Bytes.toBytes("desc"), Bytes.toBytes("这不是故事，这是真人真事儿，是咱们老百姓自己讲自己的故事"));
        puts.add(put);
        Table table = connection.getTable(TableName.valueOf("article"));
        table.put(puts);
    }
    
    @Test
    public void get() throws IOException {
        Get get = new Get(Bytes.toBytes("wenxue-001"));
        get.addColumn(Bytes.toBytes("content"), Bytes.toBytes("desc"));
        Table table = connection.getTable(TableName.valueOf("article"));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        KeyValue current = (KeyValue) cells[0];
        String row = CellUtil.getCellKeyAsString(current);
        System.out.println("rowKey: " + row);

        byte[] familyArray = CellUtil.cloneFamily(current);
        String family = Bytes.toString(familyArray);
        System.out.println("family: " + family);
        
        byte[] qualifierArray = CellUtil.cloneQualifier(current);
        String qualifier = Bytes.toString(qualifierArray);
        System.out.println("qualifier: " + qualifier);
        
        byte[] valueArray = CellUtil.cloneValue(current);
        String value = Bytes.toString(valueArray);
        System.out.println("value: " + value);
    }
    
    @Test
    public void scanner() throws IOException {
        Table table = connection.getTable(TableName.valueOf("article"));
        Scan scan = new Scan();
        // 这里限制查询范围
        scan.setStartRow(Bytes.toBytes("li"));
        scan.setStopRow(Bytes.toBytes("z"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {
            byte[] value = row.getValue(Bytes.toBytes("content"), Bytes.toBytes("desc"));
            System.out.println("value: " + Bytes.toString(value));
        }
    }
    
    @Test
    public void singleFilter() throws IOException {
        FilterList list = new FilterList();
        list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("bref"), Bytes.toBytes("author"), 
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("lisi")));
        Scan scan = new Scan();
        scan.setFilter(list);
        Table table = connection.getTable(TableName.valueOf("article"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {
            byte[] value = row.getValue(Bytes.toBytes("content"), Bytes.toBytes("desc"));
            System.out.println("value: " + Bytes.toString(value));
        }

    }

    /**
     * 获取rowkey以wenxue开头的行记录
     * @throws IOException
     */
    @Test
    public void prefixFilter() throws IOException {
        Scan scan = new Scan();
        PrefixFilter filter = new PrefixFilter(Bytes.toBytes("wenxue"));
        scan.setFilter(filter);
        Table table = connection.getTable(TableName.valueOf("article"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {
            byte[] value = row.getValue(Bytes.toBytes("content"), Bytes.toBytes("desc"));
            System.out.println("value: " + Bytes.toString(value));
        }
    }

    /**
     * 列前缀，返回列名包含author的前缀的所有记录
     * @throws IOException
     */
    @Test
    public void columnPrefixFilter() throws IOException {
        Scan scan = new Scan();
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("author"));
        scan.setFilter(filter);
        Table table = connection.getTable(TableName.valueOf("article"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {
            byte[] value = row.getValue(Bytes.toBytes("content"), Bytes.toBytes("desc"));
            System.out.println("value: " + Bytes.toString(value));
        }
    }
    
    @Test
    public void rowkeyFilter() throws IOException {
        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^wenxue")));
        Table table = connection.getTable(TableName.valueOf("article"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {
            byte[] value = row.getValue(Bytes.toBytes("content"), Bytes.toBytes("desc"));
            System.out.println("value: " + Bytes.toString(value));
        }
    }
    
}
