package com.example.hadoop;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import javax.annotation.Resource;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.util.Assert;

import com.example.hadoop.mr.ContentStatsExample;

@ComponentScan
@EnableAutoConfiguration
public class HdfsApplication implements CommandLineRunner {
	
	private static Logger logger = Logger.getLogger(ContentStatsExample.class);

	@Resource(name = "hadoopFs")
	private FileSystem fileSystem;
	
	@Autowired
	DataStoreWriter<String> writer;
	
    @Value("${hdfs.output.file}")
	private String outputFile;
    
    @Value("${input.directory}")
   	private String inputDirectory;
	
	private FSDataOutputStream fsDataOutputStream;
	
	private int count;

	@Override
	public void run(String... args) throws Exception {
		
		//writeByDataStore();
		writeByFileSystem();

	}

	private void writeByDataStore() throws IOException {
		File f = new File(inputDirectory);
		processFileByDataStore(f);
		
	}

	private void processFileByDataStore(File file) throws IOException {
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				processFileByDataStore(f);
			}
		} else {
			List<String> entries = unzipFile(file.getAbsolutePath());
			++count;
			logger.info(String.format("processing  %d file, the file name is %s", count, file.getName()));
			//byte[] bts = Files.readAllBytes(file.toPath());
			for(String entry : entries) {
				writer.write(entry);
			}
		}
		
	}

	private void writeByFileSystem() throws IOException {
		Path destFile = new Path(outputFile);
		// If it doesn't exist, create it.  If it exists, return false
		if (!fileSystem.createNewFile(destFile)) {	
			logger.info("create file failed, the file already exist");
		}
		this.fsDataOutputStream = fileSystem.append(destFile);
		File f = new File(inputDirectory);
		try {
			processFile(f);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(fsDataOutputStream!=null) {
				IOUtils.closeStream(fsDataOutputStream);
			}
			fileSystem.close();
		}
	}

	private void processFile(File file) throws IOException {
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				processFile(f);
			}
		} else {
			List<String> entries = unzipFile(file.getAbsolutePath());
			++count;
			logger.info(String.format("processing  %d file, the file name is %s", count, file.getName()));
			//byte[] bts = Files.readAllBytes(file.toPath());
			for(String entry : entries) {
				fsDataOutputStream.write(entry.getBytes());
				fsDataOutputStream.write("\n".getBytes());
			}
		}

	}

	public static void main(String[] args) {
		SpringApplication.run(HdfsApplication.class, args);
	}


	
	 private List<String> unzipFile(String filePath) throws IOException {

	        List<String> entries = new ArrayList<>();

	        ZipFile zip = null;
	        ZipInputStream zin = null;
	        try {
	            zip = new ZipFile(filePath);
	            zin = new ZipInputStream(new BufferedInputStream(new FileInputStream(filePath)));

	            ZipEntry zEntry;
	            if ((zEntry = zin.getNextEntry()) != null) {
	            	
	                logger.info(String.format("zip entry in zip file: %s, name: %s, size: %d", filePath,  zEntry.getName(),zEntry.getSize())); 
	                      
	                BufferedReader br = new BufferedReader(new InputStreamReader(zip.getInputStream(zEntry)));
	                String line;
	                while ((line = br.readLine()) != null) {
	                	logger.info(String.format("the entry is %s",line));
	                    entries.add(line);
	                }
	            }
	        } finally {
	            if (null != zip) {
	            	org.apache.commons.io.IOUtils.closeQuietly(zip);
	            }
	            if (null != zin) {
	            	org.apache.commons.io.IOUtils.closeQuietly(zin);
	            }
	        }

	        Assert.notEmpty(entries, "no content found in uploaded zip file");

	        return entries;
	    }


}
