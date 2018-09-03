package com.example.hadoop;

import com.example.hadoop.hdfs.jdbc.MysqlImportService;
import org.apache.log4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

import javax.annotation.Resource;

@ComponentScan({"com.example.hadoop.hdfs","com.example.hadoop.config"})
@EnableAutoConfiguration
public class ImportHdfsApplication implements CommandLineRunner {

    private static Logger logger = Logger.getLogger(ImportHdfsApplication.class);

    @Resource(name = "mysqlImportSrv")
    private MysqlImportService importService;

    //private FSDataOutputStream fsDataOutputStream;

    @Override
    public void run(String... args) throws Exception {
        importService.importClientStaionToTextFile();
        //importService.importNmsAccountToTextFile();
    }

    public static void main(String[] args) {
        SpringApplication.run(ImportHdfsApplication.class, args);
    }
}
