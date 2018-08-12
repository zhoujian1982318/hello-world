package hello;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class GreetingController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    @RequestMapping(value="/greeting")
    public Greeting greeting(@RequestParam(value="name", defaultValue="World") String name) {
        return new Greeting(counter.incrementAndGet(),
                            String.format(template, name));
    }

    @RequestMapping(value="/testAsyn",method= RequestMethod.GET)
    public void testAsny(HttpServletRequest request, HttpServletResponse response) throws IOException {
        System.out.println("start.....");
        PrintWriter out = response.getWriter();
        AsyncContext asyncContext = request.startAsync();
        asyncContext.setTimeout(30000);
        asyncContext.addListener(new MyAsyncListener());

        BusinessThread busWork = new BusinessThread(asyncContext);
        asyncContext.start(busWork);
        Date date = new Date(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss Z");
        out.println("异步执行中");
        out.println("Servlet  begin --" + sdf.format(date) + "<br>");// 响应输出到客户端
    }
}