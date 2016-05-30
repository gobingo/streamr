package com.ssatapathy

import com.ssatapathy.streamr._
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.concurrent.TimeUnit
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.core.env.Environment
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import org.springframework.web.client.RestTemplate

@SpringBootApplication
@EnableConfigurationProperties
class StreamrApplication extends LazyLogging {

  @Autowired
  var environment: Environment = _

  @Bean
  def restTemplate(): RestTemplate = {
    val timeout = TimeUnit.MINUTES.toMillis(2).toInt
    val requestFactory = new HttpComponentsClientHttpRequestFactory
    requestFactory.setConnectTimeout(timeout)
    requestFactory.setReadTimeout(timeout)
    new RestTemplate(requestFactory)
  }

}

object StreamrApplication {

  def main(args: Array[String]) {
    val sources: Array[AnyRef] = Array(classOf[StreamrApplication])
    val context = SpringApplication.run(sources, args)
    context.getBean(classOf[TwitterStreamr]).stream()
  }

}
