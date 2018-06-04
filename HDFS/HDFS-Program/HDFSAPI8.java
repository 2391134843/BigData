package BigDatework2;
/**
 * 8 向HDFS中指定的文件追加内容，由用户指定内容追加到原有文件的开头或结尾；
 * @author dblab
 *
 */
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class HDFSAPI8 {
		/**
		* 判断路径是否存在
		*/
		public static boolean test(Configuration conf, String path) throws IOException {
				FileSystem fs = FileSystem.get(conf);
				return fs.exists(new Path(path));
		}
		/**
		* 追加文本内容
		*/
		public static void appendContentToFile(Configuration conf, String content, String
		remoteFilePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path remotePath = new Path(remoteFilePath);
		/* 创建一个文件输出流,输出的内容将追加到文件末尾 */
		FSDataOutputStream out = fs.append(remotePath);
		out.write(content.getBytes());
		out.close();
		fs.close();
		}
		/**
		* 追加文件内容
		*/
		public static void appendToFile(Configuration conf, String localFilePath,String
		remoteFilePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path remotePath = new Path(remoteFilePath);
		/* 创建一个文件读入流 */
		FileInputStream in = new FileInputStream(localFilePath);
		/* 创建一个文件输出流,输出的内容将追加到文件末尾 */
		FSDataOutputStream out = fs.append(remotePath);
		/* 读写文件内容 */
		byte[] data = new byte[1024];
		int read = -1;
		while ( (read = in.read(data)) > 0 ) {
		out.write(data, 0, read);
		}
		out.close();
		//localFilePath,
		
		in.close();
		fs.close();
		}
		/**
		* 移动文件到本地
		* 移动后,删除源文件
		*/
		public static void moveToLocalFile(Configuration conf, String remoteFilePath, String
		localFilePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path remotePath = new Path(remoteFilePath);
		Path localPath = new Path(localFilePath);
		fs.moveToLocalFile(remotePath, localPath);
		}
		/**
		* 创建文件
		*/
		public static void touchz(Configuration conf, String remoteFilePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path remotePath = new Path(remoteFilePath);
		FSDataOutputStream outputStream = fs.create(remotePath);
		outputStream.close();
		fs.close();
		}
		//
		/**
		* 主函数
		*/
		public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://localhost:9000");
		String remoteFilePath = "text.txt";
		String content = "新追加的内容\n";
		String choice = "after";
		//追加到文件末尾
//		String choice = "before";
		// 追加到文件开头
		// HDFS 文件
		try {
		/* 判断文件是否存在 */
		if ( !HDFSAPI8.test(conf, remoteFilePath) ) {
		System.out.println("文件不存在: " + remoteFilePath);
		} else {
		if ( choice.equals("after") ) { // 追加在文件末尾
	//	19厦门大学林子雨编著《大数据基础编程、实验和案例教程》中收录的 5 个实验答案
			HDFSAPI8.appendContentToFile(conf, content, remoteFilePath);
		System.out.println("已追加内容到文件末尾" + remoteFilePath);
		} else if ( choice.equals("before") ) { // 追加到文件开头
		/* 没有相应的 api 可以直接操作,因此先把文件移动到本地*/
		/*创建一个新的 HDFS,再按顺序追加内容 */
		String localTmpPath = "/user/hadoop/tmp.txt";
		// 移动到本地
		HDFSAPI8.moveToLocalFile(conf, remoteFilePath, localTmpPath);
		// 创建一个新文件
		HDFSAPI8.touchz(conf, remoteFilePath);
		// 先写入新内容
		HDFSAPI8.appendContentToFile(conf, content, remoteFilePath);
		// 再写入原来内容
		HDFSAPI8.appendToFile(conf, localTmpPath, remoteFilePath);
		System.out.println("已追加内容到文件开头: " + remoteFilePath);
		}
		}
		} catch (Exception e) {
				e.printStackTrace();
		}
		}
}
