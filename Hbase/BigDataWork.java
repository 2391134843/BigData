
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
 
public class BigDataWork {
	public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    
    public static void main(String []args) {
    	try{
    		/*
    		 getData("student");
    		  listTables();
    		  */    		
    	
    		
    		
    		
//    		insertRow("student","zhangsan","course","Math","69");
//    		System.out.println("成功添加记录 ！");
 //	getData("student");
//    		deleteRow("student","zhangsan","course","Math");
//    		getData("student");
//    		System.out.print("成功删除记录");
    		clearRows("student2");
    //		countRows("student2");
    	}catch(Exception ex){
    		ex.printStackTrace();
    	}
    	
    }
    
    /**
     * 1.显示现在建立的所有的
     * @throws IOException
     */
    public static void listTables() throws IOException {
    	init();//建立连接
    	HTableDescriptor hTableDescriptors[] = admin.listTables();
    	for(HTableDescriptor hTableDescriptor :hTableDescriptors){
    	System.out.println("表名:"+hTableDescriptor.getNameAsString());
    	}
    	close();//关闭连接
    	}
	
  //建立连接
    public static void init(){
        configuration  = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    //关闭连接
    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
 /**
  * 2.在终端打印出指定的表的所有记录数据
  */
    public static void getData(String tableName)throws IOException{
    		init();
		    Table table = connection.getTable(TableName.valueOf(tableName));
		    Scan scan = new Scan();
		    ResultScanner scanner = table.getScanner(scan);
		    for (Result result:scanner){
		    printRecoder(result);
	    }
	    close();
    }
    //打印一条记录的详情
    public static void printRecoder(Result result)throws IOException{
	    	for(Cell cell:result.rawCells()){
			    System.out.print("行健: "+new String(CellUtil.cloneRow(cell)));
			    System.out.print("列簇: "+new String(CellUtil.cloneFamily(cell)));
			    System.out.print(" 列: "+new String(CellUtil.cloneQualifier(cell)));
			    System.out.print(" 值: "+new String(CellUtil.cloneValue(cell)));
			    System.out.println("时间戳: "+cell.getTimestamp());
	    }
    }
    /**
     * 3 向已经创建好的表添加和删除指定的列族或列
     */

    //向表添加数据
    public static void insertRow(String tableName,String rowKey,String colFamily,String col,String val) throws IOException {
    			init();
    			Table table = connection.getTable(TableName.valueOf(tableName));
    			Put put = new Put(rowKey.getBytes());
    			put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
    			table.put(put);
    			table.close();
    			close();
    }

  //删除数据
    public static void deleteRow(String tableName,String rowKey,String colFamily,String col) throws  IOException {
	    init();
	    Table table = connection.getTable(TableName.valueOf(tableName));
	    Delete delete = new Delete(rowKey.getBytes());
	    //删除指定列族
	    delete.addFamily(Bytes.toBytes(colFamily));
	    //删除指定列
	    delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
	    table.delete(delete);
	    table.close();
	    close();
    }
    
    /**
     * 4. 清空指定的表的所有记录数据;
     * @param tableName
     * @throws IOException
     */
  //清空指定的表的所有记录数据
    public static void clearRows(String tableName)throws IOException{
    init();
    TableName tablename = TableName.valueOf(tableName);
    admin.disableTable(tablename);
    admin.deleteTable(tablename);
	HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
    admin.createTable(hTableDescriptor);
    System.out.println("成功删除 " +tableName+" 表的所有记录");
    close();
    }
    
    /**
     * 5.统计表的行数
     * @param tableName
     * @throws IOException
     */
    public static void countRows(String tableName)throws IOException{
    	init();
    	Table table = connection.getTable(TableName.valueOf(tableName));
    	Scan scan = new Scan();
    	ResultScanner scanner = table.getScanner(scan);
    	int num = 0;
    	for (Result result = scanner.next();result!=null;result=scanner.next()){
    	num++;
    	}
    	System.out.println("行数:"+ num);
    	scanner.close();
    	close();
    	}
    
    
    
    
}
