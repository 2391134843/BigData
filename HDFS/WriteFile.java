import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class WriteFile {
		public static void main(String []args){
			try{
				Configuration conf = new Configuration();
				conf.set("fs.defaultFS","hdfs://localhost:9000");
				conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
				FileSystem fs = FileSystem.get(conf);
				byte[]buff = "hello hadoop and hdfs ".getBytes();
				String filename = "myFile2.txt";
				FSDataOutputStream os=fs.create(new Path(filename));
				os.write(buff,0,buff.length);
				System.out.println("Create Succeeful "+filename);
				os.close();
				fs.close();
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
}
