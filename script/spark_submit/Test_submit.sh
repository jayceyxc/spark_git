#!/bin/sh

spark-submit --master spark://192.168.1.110:7077 --class com.linus.programmer_guide.Test spark_git.jar
