package com.ibm.dataservices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

//@RestController
@SpringBootApplication
@ComponentScan("com")
public class DataserviceApp {

    //@RequestMapping(value = "/", produces = MediaType.TEXT_HTML_VALUE)
    //public String home() {
    //    return "Nothing here. Go to <a href='/sample'>/sample</a>";
    //}
    static public void main(String[] args) throws Exception
    {
        SpringApplication.run(DataserviceApp.class, args);
    }

}
