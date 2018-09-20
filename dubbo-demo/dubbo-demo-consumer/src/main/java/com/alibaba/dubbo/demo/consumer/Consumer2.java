package com.alibaba.dubbo.demo.consumer;

import com.alibaba.dubbo.demo.DemoService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author cvictory ON 2018/9/20
 */
public class Consumer2 {

    public static void main(String[] args) {
        //Prevent to get IPV6 address,this way only work in debug mode
        //But you can pass use -Djava.net.preferIPv4Stack=true,then it work well whether in debug mode or not
        System.setProperty("java.net.preferIPv4Stack", "true");
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"META-INF/spring/dubbo-demo2-consumer.xml"});
        context.start();
        ConsumerService consumerService = (ConsumerService) context.getBean(ConsumerService.class); // get remote service proxy

        while (true) {
            try {
                Thread.sleep(1000);
                consumerService.testReference(); // call remote method

            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }


        }

    }
}
