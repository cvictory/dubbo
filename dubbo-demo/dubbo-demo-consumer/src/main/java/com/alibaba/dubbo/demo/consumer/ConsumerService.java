package com.alibaba.dubbo.demo.consumer;

import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.dubbo.demo.DemoService;
import org.springframework.stereotype.Service;

/**
 * @author cvictory ON 2018/9/20
 */
@Service
public class ConsumerService {

    @Reference(parameters = {"k1", "v1"})
    private DemoService demoService;

    public void testReference() {
        System.out.println(demoService.sayHello("www.world"));
    }
}
