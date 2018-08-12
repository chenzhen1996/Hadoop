package hadoopHDFS;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;

public class HadoopHDFSTest {

	public static void main(String[] args) {
		// TODO 自动生成的方法存根

		try {
	//		URLCat();
	//		APICat();
	//		CreatDir();
	//		CreatPath();
	//		delete();
			digui(new Path("/"));
		} catch (Exception e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
	}

	//通过URL方式读取HDFS文件
	public static void URLCat() throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		URL url=new URL("hdfs://192.168.223.100:8020/user/centos/hadoop/test.sh");
		URLConnection conn=url.openConnection();
		InputStream is=conn.getInputStream();
		byte[] buff=new byte[is.available()];
		is.read(buff);
		is.close();
		System.out.println(new String(buff));
	}
	
	//通过Hadoop API方式
	public static void APICat() throws Exception {
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.223.100:8020");
		Path path=new Path("/user/centos/hadoop/test.sh");
		FileSystem fs=FileSystem.get(conf);
		InputStream is=fs.open(path);
		IOUtils.copy(is, System.out);
		is.close();
	}
	
	//创建文件夹
	public static void CreatDir() throws Exception {
		Configuration conn=new Configuration();
		conn.set("fs.defaultFS", "hdfs://192.168.223.100:8020");
		FileSystem fs=FileSystem.get(conn);
		fs.mkdirs(new Path("/user/centos/myhadoop"));
	}
	
	//put
		public static void CreatPath() throws Exception {
			Configuration conn=new Configuration();
			conn.set("fs.defaultFS", "hdfs://192.168.223.100:8020");
			FileSystem fs=FileSystem.get(conn);
			OutputStream os=fs.create(new Path("/user/centos/myhadoop/hello.txt"));
			String h="hello";
			os.write(h.getBytes());
			os.flush();
			os.close();
		}
	//删除
	public static void delete() throws Exception {
		Configuration conn=new Configuration();
		conn.set("fs.defaultFS", "hdfs://192.168.223.100:8020");
		FileSystem fs=FileSystem.get(conn);
		fs.delete(new Path("/user/centos/myhadoop"),true);
	}
	
	//递归输出HDFS的文件
	public static void digui(Path path) throws Exception {
		Configuration conn=new Configuration();
		conn.set("fs.defaultFS", "hdfs://192.168.223.100:8020");
		FileSystem fs=FileSystem.get(conn);
		FileStatus[] fslist=fs.listStatus(path);
		for(FileStatus f:fslist) {
			if(f.isDirectory()) {
				System.out.println(f.getPath());
				digui(f.getPath());
			}else {
			System.out.println(f.getPath());
			}
		}
			
	}
}
