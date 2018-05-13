import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * 读出hdfs文件里面所有的内容
 *
 */
public class ReadFIle {
	public static void main(String [] args){
		try{
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://localhost:9000");
			conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
			FileSystem fs = FileSystem.get(conf);
			Path file = new Path("input/myLocalFile.txt");
			
			FSDataInputStream getIt = fs.open(file);
			BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
			String content ;
			while( (content=d.readLine())!=null){
				System.out.println(content);	
			}
			
			d.close();
			fs.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
}
