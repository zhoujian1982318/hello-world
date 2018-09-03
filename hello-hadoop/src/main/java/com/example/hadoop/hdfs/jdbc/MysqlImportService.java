package com.example.hadoop.hdfs.jdbc;

import com.example.hadoop.pojo.AccountInfo;
import com.example.hadoop.pojo.ClientStationInfo;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

@Service(value = "mysqlImportSrv")
public class MysqlImportService {


    private static Logger logger = Logger.getLogger(MysqlImportService.class);

    @Resource(name = "hadoopFs")
    private FileSystem fileSystem;

    private  @Value("${account.output.file}") String accountFile;

    private  @Value("${cs.output.file}") String csFile;

    private  @Value("${ap.output.file}") String apFile;

    @Resource(name= "hadoopConfiguration")
    private org.apache.hadoop.conf.Configuration conf;

    @Resource(name = "jdbcT")
    private JdbcTemplate jdbcT;

    @Resource(name = "avroDsWriter")
    private DataStoreWriter<AccountInfo> writer;

    private int totalCount=0;

    private final String CS_SQL =  " SELECT idClientStation, idClientStationMacAddr, apMacAddr, BSSID, SSID,  idNMSAccount, radioProtocol, status, RSSI, idWLAN, radioSlotNum, cause, statusTime  FROM ClientStation "
                                + " where statusTime>? AND  statusTime<=?  AND idClientStation> ? order by idClientStation asc limit 0, 10000";

    private long getClientStation(FSDataOutputStream fsDataOutputStream, long startTime, long endTime,  long idClientStation) throws IOException {
        SqlRowSet rs = jdbcT.queryForRowSet(CS_SQL, new Object[]{startTime, endTime, idClientStation});
        long lastIdCs = 0L;
        int count = 0;
        while(rs.next()){
              lastIdCs = rs.getLong("idClientStation");
              String csMac = rs.getString("idClientStationMacAddr");
              String apMac = rs.getString("apMacAddr");
              String bssid = rs.getString("BSSID");
              String ssid = rs.getString("SSID") == null ? ""  : rs.getString("SSID") ;
              long  acctId = rs.getLong("idNMSAccount");
              int radioProtocol = rs.getInt("radioProtocol");
              int rssi = rs.getInt("RSSI");
              int sts = rs.getInt("status");
              int wlanId = rs.getInt("idWLAN");
              int raidoSlotNum = rs.getInt("radioSlotNum");
              String cause = rs.getString("cause")== null ? ""  : rs.getString("cause");
              long ts = rs.getLong("statusTime");
              String text = String.format("%s,%s,%s,\"%s\",%d,%d,%d,%d,%d,%d,%s,%d",csMac,apMac,bssid,ssid,acctId,sts,radioProtocol,rssi,wlanId,raidoSlotNum,cause,ts);
              fsDataOutputStream.write(text.getBytes());
              fsDataOutputStream.write("\n".getBytes());
              count+=1;
              totalCount+=1;
        }
        logger.info("the count is "+count);
        if(count<10000){
            return 0L ;
        }else{
            return lastIdCs;
        }

    }
    public void importClientStaionToTextFile() throws IOException, ParseException {

        Path destFile = new Path(csFile);
        if (!fileSystem.createNewFile(destFile)) {
            logger.info("create file failed, the file already exist");
        }
        FSDataOutputStream fsDataOutputStream = fileSystem.append(destFile);
        try {
            SimpleDateFormat sf  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long  startTime = sf.parse("2018-01-01 00:00:00").getTime();
            long  endTime = sf.parse("2018-02-01 00:00:00").getTime();
            logger.info("the start time is "+ startTime +",  the end time is "+ endTime);
            long lastId = getClientStation(fsDataOutputStream, startTime, endTime,0L);
            while(lastId!=0){
                lastId = getClientStation(fsDataOutputStream, startTime,endTime, lastId);
            }

            logger.info("the total count is  "+ totalCount);
        }finally {
            if(fsDataOutputStream!=null) {
                IOUtils.closeStream(fsDataOutputStream);
            }
            fileSystem.close();
        }
    }

    public void  importNmsAccountToSeqFile() throws IOException {

        Path destFile = new Path(accountFile);
        if (!fileSystem.createNewFile(destFile)) {
            logger.info("create file failed, the file already exist");
        }
        LongWritable accountIdKey = new LongWritable();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(destFile), SequenceFile.Writer.keyClass(accountIdKey.getClass()),
                SequenceFile.Writer.valueClass(value.getClass()),SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
        try {
            String sql = " SELECT idNMSAccount, idResellerAccount, accountName, accountStatus FROM NMSAccount ";
            List<Map<String, Object>> accounts = jdbcT.queryForList(sql);
            logger.info("the accounts is : " + accounts);
            for (Map<String, Object> account : accounts) {
                Long  acctId = (Long)account.get("idNMSAccount");
                Long parentId = (Long)account.get("idResellerAccount");
                String acctName = (String)account.get("accountName");
                Integer status = (Integer)account.get("accountStatus");
                String  strValue = String.format("%d,%s,%d,%d", acctId, acctName, parentId, status);
                accountIdKey.set(acctId);
                value.set(strValue);
                writer.append(accountIdKey, value);
            }
        }finally {
                IOUtils.closeStream(writer);
        }
    }


    public void  importNmsAccountToTextFile() throws IOException {
        Path destFile = new Path(accountFile);
        // If it doesn't exist, create it.  If it exists, return false
        if (!fileSystem.createNewFile(destFile)) {
            logger.info("create file failed, the file already exist");
        }
        FSDataOutputStream fsDataOutputStream = fileSystem.append(destFile);

        try {
            String sql = " SELECT idNMSAccount, idResellerAccount, accountName, accountStatus FROM NMSAccount order by accountName ";
            List<Map<String, Object>> accounts = jdbcT.queryForList(sql);
            logger.info("the accounts is : " + accounts);
            for (Map<String, Object> account : accounts) {
                Long  acctId = (Long)account.get("idNMSAccount");
                Long parentId = (Long)account.get("idResellerAccount");
                String acctName = (String)account.get("accountName");
                Integer status = (Integer)account.get("accountStatus");
                String  entry = String.format("%d,%s,%d,%d", acctId, acctName, parentId, status);
                fsDataOutputStream.write(entry.getBytes());
                fsDataOutputStream.write("\n".getBytes());

            }
        }finally {
            if(fsDataOutputStream!=null) {
                IOUtils.closeStream(fsDataOutputStream);
            }
            fileSystem.close();
        }
    }

    public void  importApToTextFile() throws IOException {

        Path destFile = new Path(apFile);
        // If it doesn't exist, create it.  If it exists, return false
        if (!fileSystem.createNewFile(destFile)) {
            logger.info("create file failed, the file already exist");
        }
        FSDataOutputStream fsDataOutputStream = fileSystem.append(destFile);

        try {
            String sql = " SELECT  idNMSAccount, apMacAddr, apName FROM AccessPoint order by apName ; ";
            List<Map<String, Object>> aps = jdbcT.queryForList(sql);

            for (Map<String, Object> ap : aps) {
                Long  acctId = (Long)ap.get("idNMSAccount");
                String apMac = (String)ap.get("apMacAddr");
                String apName = (String)ap.get("apName");
                String  entry = String.format("%d,%s,%s", acctId, apMac,  apName);
                fsDataOutputStream.write(entry.getBytes());
                fsDataOutputStream.write("\n".getBytes());

            }
        }finally {
            if(fsDataOutputStream!=null) {
                IOUtils.closeStream(fsDataOutputStream);
            }
            fileSystem.close();
        }
    }

    public void importNmsAccountToAvroFile() throws IOException {
        try {
            String sql = " SELECT idNMSAccount, idResellerAccount, accountName, accountStatus FROM NMSAccount ";
            List<Map<String, Object>> accounts = jdbcT.queryForList(sql);
            logger.info("the accounts is : " + accounts);
            for (Map<String, Object> account : accounts) {
                Long  acctId = (Long)account.get("idNMSAccount");
                Long parentId = (Long)account.get("idResellerAccount");
                String acctName = (String)account.get("accountName");
                Integer status = (Integer)account.get("accountStatus");
                AccountInfo accountInfo = new AccountInfo();
                accountInfo.setAccountId(acctId);
                accountInfo.setParentId(parentId);
                accountInfo.setAccountName(acctName);
                accountInfo.setStatus(status);
                writer.write(accountInfo);

            }
        }finally {
            writer.close();
        }
    }

}
