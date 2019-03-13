package hello;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class HttpRestClient {
	private Logger logger = LoggerFactory.getLogger(HttpRestClient.class);
	
    private HttpClient httpClient = null;
    
    private static HttpRestClient client = new HttpRestClient();

    private HttpRestClient() {
        init();
    }

    public static HttpRestClient getInstance() {
        return client;
    }

    /**
     * ��ʼ��HTTP Client
     */
    private void init() {
        try {
            
//            SSLContextBuilder builder = SSLContexts.custom();
//            SSLContext sslContext = builder.build();
//           HostnameVerifier verifier = SSLConnectionSocketFactory.getDefaultHostnameVerifier();
//            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, new X509HostnameVerifier() {
//                @Override
//                public void verify(String host, SSLSocket ssl) throws IOException {
//                }
//                @Override
//                public void verify(String host, X509Certificate cert) throws SSLException {
//                }
//                @Override
//                public void verify(String host, String[] cns, String[] subjectAlts) throws SSLException {
//                }
//                @Override
//                public boolean verify(String s, SSLSession sslSession) {
//                    return true;
//                }
//            });
            
//            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext);
//        	
//        	 SSLContextBuilder builder = SSLContexts.custom();
//             builder.loadTrustMaterial(null, new TrustStrategy() {
//                 @Override
//                 public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
//                     return true;
//                 }
//             });
//             SSLContext sslContext = builder.build();
//             SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, new X509HostnameVerifier() {
//                 @Override
//                 public void verify(String host, SSLSocket ssl) throws IOException {
//                 }
//                 @Override
//                 public void verify(String host, X509Certificate cert) throws SSLException {
//                 }
//                 @Override
//                 public void verify(String host, String[] cns, String[] subjectAlts) throws SSLException {
//                 }
//                 @Override
//                 public boolean verify(String s, SSLSession sslSession) {
//                     return true;
//                 }
//             });


//            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory> create()
//                                                                                       .register("https", sslsf).build();
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory> create().register("http", PlainConnectionSocketFactory.getSocketFactory()).build();
                    

            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            httpClient = HttpClients.custom().setConnectionManager(cm).build();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * ��ȡ authCode���㷨
     * 
     * @param msgString �������
     * @param authKey �ͻ���Authentication Key
     * 
     * @return
    */
    private String genAuthCode(String msgString, String authKey) {
        try {
            String HMAC_SHA1_ALGORITHM = "HmacSHA1";
            SecretKeySpec key = new SecretKeySpec(authKey.getBytes(), HMAC_SHA1_ALGORITHM);
            Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
            mac.init(key);
            byte[] rawHmac = mac.doFinal(msgString.getBytes());
            String authCode = javax.xml.bind.DatatypeConverter.printBase64Binary(rawHmac);
            authCode = authCode.toLowerCase();
            logger.info("### message string: [" + msgString + "] -> [" + authCode + "]");

            return authCode;
        } catch (NoSuchAlgorithmException ex) {
            logger.error("NoSuchAlgorithmException", ex);
        } catch (InvalidKeyException ex) {
            logger.error("InvalidKeyException", ex);
        }
        return null;
    }
    
    /**
     * 初始化HTTP Client
     */
    public void testGetWlanList(){
        CloseableHttpResponse response = null;
        try {
            final String reqId = "1111-3333-555";
            final String acctName = "WS-API-Test";
            final String actor = "R@r2.com";
            final long ts = System.currentTimeMillis();
            final String accessKey = "FEB7D36C923344159EA3BED115262288".toUpperCase();
            final String authKey = "BBAD1F7154A64607ABB1A1D6C5AB1D13";
            final String action = ADD_WLAN;
            final String version = "2014.08.15";

            String msgString = String.format("%d~%s~%s~%s~%s~%s~%s", ts, version, acctName, accessKey, action, actor,
                    reqId);
            String authCode = genAuthCode(msgString, authKey);
            System.out.println(authCode);
            
            logger.info("Start getWlanList...");
            
//            HttpUriRequest request = RequestBuilder.post(WS_URL.concat(ADD_WLAN))
//            					   .setCharset(Charset.forName("UTF-8"))
//            		               .addParameter("accountName", acctName)
//                                   .addParameter("reqId", reqId)
//                                   .addParameter("actor", actor)
//                                   .addParameter("accessKey", accessKey)
//                                   .addParameter("action", ADD_WLAN)
//                                   .addParameter("ts", String.valueOf(ts))
//                                   .addParameter("version",version)
//                                   .addParameter("authCode",authCode)
//                                   .addParameter("wlanName","测试1")
//                                   .addParameter("SSID","testwlan11")
//                                   .addParameter("adminStatus","2")
//                                   .addParameter("wlanType","1")
//                                   .addParameter("portalUrl","https://www.mycompany.com/portal.php")
//                                   .addParameter("idBanner","1")
//                                   .build();
            
            HttpUriRequest request = RequestBuilder.post(WS_URL.concat("Ws"+ADD_WLAN)).setCharset(Charset.forName("UTF-8")).addParameter("accountName", acctName)
                    .addParameter("reqId", reqId)
                    .addParameter("actor", actor)
                    .addParameter("accessKey", accessKey)
                    .addParameter("action", ADD_WLAN)
                    .addParameter("ts", String.valueOf(ts))
                    .addParameter("version",version)
                    .addParameter("authCode",authCode)
                    .addParameter("idWlan","1")
                    .addParameter("wlanName","测试99")
                    .addParameter("SSID","test777")
                    .addParameter("portalUrl","https://www.mycompany.com/portal2.php")
                    .addParameter("bnFileName","banner-test.zip")
                    .build();
            
            //request.setHeader("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
            request.setHeader("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
//          request.setHeader("Content-Type", "application/json; charset=UTF-8");
            
            request.setHeader("Accept", "application/json");
            
            response = (CloseableHttpResponse) httpClient.execute(request);

            HttpEntity entity = response.getEntity();
            
            System.out.println(GET_WLAN_LIST + " get: " + response.getStatusLine().getStatusCode() + ", content: \n" + EntityUtils.toString(entity));
            EntityUtils.consume(entity);

        } catch (Exception ex) {
            logger.error("testGetWlanList got exception", ex);
        } finally {
            if(null != response){
                HttpClientUtils.closeQuietly(response);
            }
        }
    }
    
    private static final String GET_WLAN_LIST = "GetWlanList";
    private static final String ADD_WLAN = "AddWlan";
    private static final String DELETE_BANNER = "DeleteBannerInsertion";
    private static final String UPDATE_WLAN = "UpdateWlan";
    private static final String WS_URL = "http://127.0.0.1:8080/nms/exnms/";
    
    
    public static void main(String[] args) {
        HttpRestClient client = HttpRestClient.getInstance();
        
        client.testGetWlanList();
    }


}

