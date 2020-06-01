/**
 * FileName: PageOneStepConvertRateSpark
 * Author:   86155
 * Date:     2020/5/31 16:34
 * Description:
 */

package com.ibeifeng.sparkproject.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

public class PageOneStepConvertRateSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
        SparkUtils.mockData(sc,sqlContext);

        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE));
        if(task == null){
            System.out.println(new Date() + ": task is null");
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
        JavaPairRDD<String,Row> sessionid2action = getSessionId2action(actionRDD);
        JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2action.groupByKey();

        JavaPairRDD<String,Integer> pageSplitRDD = generateAndMatchPageSplit(sc,sessionid2actionsRDD,taskParam);
    }


    private static JavaPairRDD<String, Row> getSessionId2action(JavaRDD<Row> actionRDD) {
        JavaPairRDD<String, Row> sessionid2actionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }
        });
        return sessionid2actionRDD;
    }
    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(JavaSparkContext sc,
                                                                          JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD,
                                                                          JSONObject taskParam) {
        String param = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        Broadcast<String> taskParamBoradcast = sc.broadcast(param);

        return sessionid2actionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {


                List<Tuple2<String,Integer>> list = new ArrayList<>();
                Iterator<Row> iterator = tuple._2.iterator();

                String[] splitParam = taskParamBoradcast.value().split(",");

                List<Row> row = new ArrayList<>();

                while (iterator.hasNext()){
                    row.add(iterator.next());
                }
                //以时间维度进行排序
                Collections.sort(row, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String date1 = o1.getString(4);
                        String date2 = o2.getString(4);

                        Date actionTime1 = DateUtils.parseTime(date1);
                        Date actionTime2 = DateUtils.parseTime(date2);

                        return (int)(actionTime1.getTime() - actionTime2.getTime());
                    }
                });
            Long lastPageId = null;

            for (Row row1 : row){
                Long pageId = row1.getLong(3);
                if(lastPageId == null){
                    lastPageId = pageId;
                    continue;
                }
                String pageSplit = lastPageId + "_" + pageId;
                for (int i = 1;i < splitParam.length;i++){
                    String targetPageSplit = splitParam[i-1] + "_" + splitParam[i];
                    if (pageSplit.equals(targetPageSplit)){
                        list.add(new Tuple2<>(pageSplit,1));
                        break;
                    }
                }
            lastPageId = pageId;
            }
            return list;
            }
        });


    }

}
