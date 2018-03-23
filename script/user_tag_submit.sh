#!/usr/bin/env bash
JOB_DAY=$1
OUT_PATH=$2
IN_PATH=$3

OUT_PATH=${OUT_PATH}/${JOB_DAY}

echo ${IN_PATH} ${OUT_PATH}

#spark-submit --class com.linus.yinni.UserTag --files data/url_tags.txt,data/domain.txt --jars lib/aho-corasick-double-array-trie-1.1.0.jar,lib/commons-text-1.2.jar,lib/commons-lang3-3.7.jar spark_git.jar hdfs://192.168.1.110:8020/user/yuxuecheng/yinni_visit/20180202/visit_2018020200.log.gz hdfs://192.168.1.110:8020/user/yuxuecheng/spark/yinni_user_tag/20180202
spark-submit --class com.linus.yinni.UserTag --files data/url_tags.txt,data/domain.txt --jars lib/aho-corasick-double-array-trie-1.1.0.jar,lib/commons-text-1.2.jar,lib/commons-lang3-3.7.jar spark_git.jar ${IN_PATH} ${OUT_PATH}
echo "Totally ${JOB_DAY} done!!"
exit $?
