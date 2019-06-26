package cn.spark.study.mlib.akg

import java.util.Random
import java.io.File

object Config {
  
  def log4Path =Thread.currentThread().getContextClassLoader.getResource("log4j.properties").getPath;

  def BF_DIR: String = {
    val parentFile = new File(log4Path).getParentFile();
    if (parentFile != null && parentFile.getParentFile != null) {
      parentFile.getParentFile.getPath
    } else {
      "file://d:";
    }
  }
  def CUR_PRJ_GIT: String = BF_DIR + "/git"
  
  def CUR_PRJ_DOC: String = BF_DIR + "/doc"
  
  def main(args: Array[String]): Unit = {
      println(BF_DIR)
  }  
 
}