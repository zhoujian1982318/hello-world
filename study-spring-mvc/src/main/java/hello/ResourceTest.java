package hello;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

public class ResourceTest {

    public static void main(String[] args) throws IOException {
//        URL url = ResourceTest.class.getResource("/test.properties");
//        URL url = ResourceTest.class.getClassLoader().getResource("test.properties");
        Properties p = new Properties();
        //p.load(url.openStream());
        p.load(ResourceTest.class.getResourceAsStream("/test.properties"));
        System.out.println(p);
    }
}
