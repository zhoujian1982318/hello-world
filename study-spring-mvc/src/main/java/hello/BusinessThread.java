package hello;

import javax.servlet.AsyncContext;
import javax.servlet.ServletResponse;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BusinessThread implements Runnable  {

    // 异步操作的上下文对象,通过构造方法传进来
    private AsyncContext asyncContext;

    public BusinessThread(AsyncContext asyncContext){
        this.asyncContext = asyncContext;
    }
    @Override
    public void run() {
        try {
            // do some work...
            Thread.sleep(8000); // 和browser的timeout时间相关。如果browser上timeout是30s，则大于30时，网络已经断开。这是就会异常。

            ServletResponse response = asyncContext.getResponse();
            PrintWriter out = response.getWriter();
            Date date = new Date(System.currentTimeMillis());
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss Z");

            out.println("business worker finished --"+ sdf.format(date));// 响应输出到客户端
            // 告诉启动异步处理的Servlet异步处理已完成，Servlet就会提交请求响应
            asyncContext.complete();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
