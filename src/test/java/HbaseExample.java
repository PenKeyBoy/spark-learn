import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import static java.lang.System.out;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

/**
 * Created by jinwe_000 on 2018/3/19.
 */
public class HbaseExample {
    //define the main method
    public static void main(String[] args) throws IOException {
        //set log level
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(config);
        //define table schema
        HTableDescriptor htd = new HTableDescriptor("test");
        HColumnDescriptor htc = new HColumnDescriptor("data");
        htd.addFamily(htc);
        admin.createTable(htd);
        byte[] tableName = htd.getName();
        HTableDescriptor[] tableList = admin.listTables();
        if(tableName.length != 1 && Bytes.equals(tableName,tableList[0].getName())){
            throw new IOException("create table failed");
        }
        //table operator
        HTable table = new HTable(config,tableName);
        byte[] row1 = Bytes.toBytes("row1");
        Put p = new Put(row1);
        byte[] dataTypes = Bytes.toBytes("data");
        byte[] qualifier = Bytes.toBytes("1");
        byte[] value = Bytes.toBytes("value1");
        p.add(dataTypes, qualifier, value);
        table.put(p);
        //get cell info
        Get getInfo = new Get(row1);
        Result result = table.get(getInfo);
        out.println("table info: "+result);
        //scan the table information
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        try{
            for(Result scanner:resultScanner){
                out.println("table cell infomation scan result: "+scanner);
            }
        }finally{
            resultScanner.close();
        }
        //delete table
        admin.disableTable(tableName);  //disble the table first
        admin.deleteTable(tableName);

    }
}