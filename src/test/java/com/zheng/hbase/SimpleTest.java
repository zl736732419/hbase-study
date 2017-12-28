package com.zheng.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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
    
    
}
