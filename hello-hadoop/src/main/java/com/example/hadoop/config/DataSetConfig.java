package com.example.hadoop.config;

import com.example.hadoop.pojo.AccountInfo;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.dataset.AvroPojoDatasetStoreWriter;
import org.springframework.data.hadoop.store.dataset.DatasetDefinition;
import org.springframework.data.hadoop.store.dataset.DatasetRepositoryFactory;

import javax.annotation.Resource;

@Configuration
public class DataSetConfig {

    @Resource(name= "hadoopConfiguration")
    private org.apache.hadoop.conf.Configuration hadoopConfiguration;

    @Bean
    public DatasetRepositoryFactory datasetRepositoryFactory() {
        DatasetRepositoryFactory datasetRepositoryFactory = new DatasetRepositoryFactory();
        datasetRepositoryFactory.setConf(hadoopConfiguration);
        datasetRepositoryFactory.setBasePath("/mgmt/account");
        datasetRepositoryFactory.setNamespace("test");
        return datasetRepositoryFactory;
    }


    @Bean(name="avroDsWriter")
    public DataStoreWriter<AccountInfo> dataStoreWriter() {
        AvroPojoDatasetStoreWriter datasetStoreWriter = new AvroPojoDatasetStoreWriter<AccountInfo>(AccountInfo.class, datasetRepositoryFactory(), fileInfoDatasetDefinition());


        return datasetStoreWriter;
    }

    @Bean
    public DatasetDefinition fileInfoDatasetDefinition() {
        DatasetDefinition definition = new DatasetDefinition();
        definition.setFormat(Formats.AVRO.getName());
        definition.setTargetClass(AccountInfo.class);
        definition.setAllowNullValues(false);
        return definition;
    }


}
