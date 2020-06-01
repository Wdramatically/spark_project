/**
 * FileName: SparkUtis
 * Author:   86155
 * Date:     2020/5/31 16:37
 * Description:
 */

package com.ibeifeng.sparkproject.util;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.MockData;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class SparkUtis {

    public static void setMaster(SparkConf conf){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            conf.setMaster("local");
        }
    }

    public static void mockData(JavaSparkContext sc, SQLContext sqlContext){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            MockData.mock(sc,sqlContext);
        }
    }

    public static SQLContext getSQLContext(SparkContext sc){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            return new SQLContext(sc);
        }else {
            return  new HiveContext(sc);
        }
    }
    private static JavaRDD<Row> getActionRDDByDateRange(
            SQLContext sqlContext, JSONObject taskParam){
        String startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String stopDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);
        String sql = "select * " +
                "from user_visit_action " +
                "where date >= '" + startDate +"' " +
                "and date <= '" + stopDate + "'";
        DataFrame actionDF = sqlContext.sql(sql);
        return actionDF.javaRDD();
    }

}
