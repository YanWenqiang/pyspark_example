
if [ $# != 1 ]; then
    echo "USAGE: $0 DAY"
    echo " e.g.: $0 20191201"
    exit 0
fi

export HADOOP_CONF_DIR=/etc/hadoop/conf
DAY=$1

TABLE=viewfs:***
INPUT=viewfs:***
OUTPUT=viewfs:***

JOB_NAME='medical_click_query_'$DAY

hadoop fs -rmr $OUTPUT

spark-submit --master yarn \
        --deploy-mode cluster \
        --name $JOB_NAME \
        --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="./py37/py37_seg/bin/python" \
        --num-executors 500 \
        --executor-memory 32g \
        --driver-memory 1g \
        --archives "viewfs://nsX/user/dmplt/andrew/tools/py37_seg.tgz#py37" \
        extract_query.py $TABLE $INPUT $OUTPUT 
