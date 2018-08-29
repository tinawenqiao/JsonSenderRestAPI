package org.cmbc.bigdata.config;

import com.google.common.base.Predicates;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
public class SwaggerConfig {
  @Bean
  public Docket api() {
    return new Docket(DocumentationType.SWAGGER_2)
            .apiInfo(apiInfo())
            .select()
            .apis(RequestHandlerSelectors.any())
            .apis(Predicates.not(RequestHandlerSelectors.basePackage("org.springframework.boot")))
            .paths(PathSelectors.any())
            .build();
  }


  private ApiInfo apiInfo() {
    return new ApiInfoBuilder()
            .title("JSON Sender REST API SwaggerUI")
            .description("JSON Sender REST API SwaggerUI")
            .contact(new Contact("tinawenqiao",
                    "https://github.com/tinawenqiao", "wenqiaotiangao@gmail.com"))
            .version("0.1.0")
            .build();
  }

}
