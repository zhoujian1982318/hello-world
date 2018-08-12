package hello;

import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;

import java.io.IOException;

public class MyAsyncListener implements AsyncListener {
    @Override
    public void onComplete(AsyncEvent event) throws IOException {

        System.out.println("异步servlet【onComplete完成】");
    }

    @Override
    public void onTimeout(AsyncEvent event) throws IOException {
        System.out.println("异步servlet【timeout】");
    }

    @Override
    public void onError(AsyncEvent event) throws IOException {

    }

    @Override
    public void onStartAsync(AsyncEvent event) throws IOException {

    }
}
