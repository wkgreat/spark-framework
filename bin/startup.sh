#!/bin/bashCURRENT_DIR=$(cd `dirname $0`; pwd)CONFIG_DIR=$CURRENT_DIR/../confspark2-submit \--jars $(echo ../lib/*.jar | tr ' ' ',') \--class wk.spark.framework.apps.olddriver.TruckGPSAnalysisApp ../lib/spark-framework-2.0.jar ${CONFIG_DIR} "$@"