package com.example.hadoop.hdfs;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.hadoop.config.annotation.EnableHadoop;
import org.springframework.data.hadoop.config.annotation.SpringHadoopConfigurerAdapter;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigConfigurer;
import org.springframework.data.hadoop.fs.FileSystemFactoryBean;

@Configuration
@ImportResource("hadoop-context.xml")
@EnableHadoop
public class FileSystemConfig  {
    //extends SpringHadoopConfigurerAdapter
//    @Value("${spring.hadoop.fsUri}")
//    private  String fileSystemUri;
//
//    @Override
//    public void configure(HadoopConfigConfigurer config) throws Exception {
//        config
//                .fileSystemUri(fileSystemUri);
//    }
//
//    @Bean
//    @Autowired
//    public FileSystem hadoopFileSystem(Configuration hadoopConfig){
//        FileSystemFactoryBean  fileSystemFactoryBean = new FileSystemFactoryBean();
//        fileSystemFactoryBean.setConfiguration(hadoopConfig);
//
//        return fileSystemFactoryBean.getObject();
//
//    }
}
