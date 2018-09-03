package com.example.hadoop.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class JdbcConfig {

    private  @Value("${mysql.connect.uri}") String connectUrl;

    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public DataSource dataSource() {
        DriverManagerDataSource driverMgmtDs = new DriverManagerDataSource();
        driverMgmtDs.setDriverClassName("com.mysql.jdbc.Driver");
        driverMgmtDs.setUrl(connectUrl);
        driverMgmtDs.setUsername("root");
        driverMgmtDs.setPassword("mysql");
        return driverMgmtDs;
    }

    @Bean(name="jdbcT")
    public JdbcTemplate jdbcTemplate() {
        DataSource ds = dataSource();
        return new JdbcTemplate(ds);
    }
}
