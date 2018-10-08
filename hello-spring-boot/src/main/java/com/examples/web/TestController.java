package com.examples.web;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/test")
public class TestController {

    private Logger LOG  = LoggerFactory.getLogger(TestController.class);

    @RequestMapping(value = "/hello", method = RequestMethod.POST, consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody
    Map<String,String> test(@RequestParam("param") String param){

//        try {
//            LOG.info("sleep 30 second");
//            TimeUnit.MILLISECONDS.sleep(30000);
//        } catch (InterruptedException e) {
//            LOG.error("sleep error", e);
//        }
        LOG.info("the parameter is {}", param);
        Map<String, String> result = new HashMap<>();
        result.put("rc","success");
        return result ;
    }
}
