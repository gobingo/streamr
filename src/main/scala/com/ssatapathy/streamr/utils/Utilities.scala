package com.ssatapathy.streamr.utils

import java.io.InputStream
import java.util.regex.Pattern
import org.springframework.stereotype.Component

@Component
class Utilities extends java.io.Serializable {

  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter() = {
    val stream : InputStream = getClass.getResourceAsStream("/twitter.txt")

    for (line <- scala.io.Source.fromInputStream(stream).getLines()) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  /** Retrieves a regex Pattern for parsing Apache access logs. */
  def apacheLogPattern():Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }

}
