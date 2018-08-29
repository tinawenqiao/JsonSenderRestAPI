package org.cmbc.bigdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@SpringBootApplication(scanBasePackages = {"org.cmbc.bigdata", "org.cmbc.bigdata.exception"})
@EnableWebMvc
public class JsonSenderApplication {
  public static void main(String[] args) {
    SpringApplication.run(JsonSenderApplication.class, args);
  }
}
