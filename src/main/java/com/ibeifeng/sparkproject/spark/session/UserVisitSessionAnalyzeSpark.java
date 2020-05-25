/**
 * FileName: UserVisitSessionAnalyzeSpark
 * Author:   86155
 * Date:     2020/5/25 10:33
 * Description:
 */

package com.ibeifeng.sparkproject.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.MockData;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.StringUtils;
import com.ibeifeng.sparkproject.util.ValidUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;


public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());

        mockData(sc,sqlContext);

        //从user_visit_action表中查询出来指定日期范围内的行为数据
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        //获取到此次spark submit时所带的参数信息：taskid
        Long task_id = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION);
        Task task = taskDAO.findById(task_id);
        if (task == null){
            System.out.println(new Date() + ": cannot find this task with id [" + task_id + "].");
            return;
        }
        //获取到task表中以JSON格式存储的param
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        //从user_visit_action表中，查询出指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = aggregateBySession(actionRDD, sqlContext);
        //重构过滤方法，同时统计合法session
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("",new SessionAggrStatAccumulator());
        JavaPairRDD<String, String> filteredSessionid2AddrStat = filterSessionAndAggrStat(sessionid2FullAggrInfoRDD, taskParam, sessionAggrStatAccumulator);


        sc.close();
    }

    /**
     * 获取SQLContext如果是在本地运行就生成SqlContext，如果实在生产环境运行就生成HiveContext对象
     *
     * @param sc
     * @return
     */
    private static SQLContext getSQLContext(SparkContext sc){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            return new SQLContext(sc);
        }else {
            return  new HiveContext(sc);
        }
    }

    private static void mockData(JavaSparkContext sc,SQLContext sqlContext){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            MockData.mock(sc,sqlContext);
        }
    }

    /**
     *
     * @param sqlContext
     * @param taskParam 任务参数
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDateRange(
            SQLContext sqlContext,JSONObject taskParam){
        String startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String stopDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);
        String sql = "select * " +
                "from user_visit_action" +
                "where date >= '" + startDate +"'" +
                "AND DATE <= '" + stopDate + "'";
        DataFrame actionDF = sqlContext.sql(sql);

        return actionDF.javaRDD();
    }

    /**
     * 
     * @param actionRdd  行为数据RDD
     * @return  session粒度聚合数据
     */
    private static JavaPairRDD<String,String> aggregateBySession(JavaRDD<Row> actionRdd,SQLContext sqlContext){
        JavaPairRDD<String,Row> sessionid2ActionRDD = actionRdd.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2),row);
            }
        });
        //按sessionid进行分组
        JavaPairRDD<String,Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();
        
        JavaPairRDD<Long,String> userid2PartAggrInfoRDD =  sessionid2ActionsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
               String sessionid = stringIterableTuple2._1;
               Long userid = null;
               Date startTime = null;
               Date endTime = null;
               int stepLength = 0;
               Iterator<Row> iterator = stringIterableTuple2._2.iterator();

               StringBuffer searchKeywordsBuffer = new StringBuffer("");
               StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

               while(iterator.hasNext()){
                   Row row = iterator.next();
                   if (userid == null){
                       userid = row.getLong(1);
                   }

                   String searchKeyword = row.getString(5);
                   Long clickCategoryId = row.getLong(6);

                   if (StringUtils.isNotEmpty(searchKeyword)) {
                       if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                           searchKeywordsBuffer.append(searchKeyword);
                       }
                   }
                   if (clickCategoryId != null){
                       if (clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
                           clickCategoryIdsBuffer.append(clickCategoryId);
                       }
                   }
                   Date activeTime = DateUtils.parseTime(row.getString(4));

                   if (startTime == null){
                       startTime = activeTime;
                   }
                   if (endTime == null){
                       endTime = activeTime;
                   }
                   if (activeTime.before(startTime)){
                       startTime = activeTime;
                   }
                   if (activeTime.after(endTime)){
                       endTime = activeTime;
                   }

                   stepLength++;
                   }
                   String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                    String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                    //访问时长计算
                    long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
                    String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|" +
                            Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
                            Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
                            Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
                            Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
                            Constants.FIELD_START_TIME + "=" + startTime;
                    //key为userid，value为session信息中需要的字段信息，方便与用户信息进行join
                    return new Tuple2<Long, String>(userid,partAggrInfo);
            }
        });
        //查询用户数据
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<Long,Row> userid2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0),row);
            }
        });
        JavaPairRDD<Long, Tuple2<Row, String>> userid2FulInfoRDD = userid2InfoRDD.join(userid2PartAggrInfoRDD);
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FulInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Row, String>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<Row, String>> value) throws Exception {
                String partAggrInfo = value._2._2;
                Row userInfoRow = value._2._1;
                String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);
                String fullAggrInfo = partAggrInfo + "|"
                        +Constants.FIELD_AGE + "=" + age + "|"
                        +Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        +Constants.FIELD_CITY + "=" + city + "|"
                        +Constants.FIELD_SEX  + "=" + sex;
                return new Tuple2<String, String>(sessionid,fullAggrInfo);
            }
        });
        return sessionid2FullAggrInfoRDD;
    }



    private  static JavaPairRDD<String,String> filterSessionAndAggrStat(
            JavaPairRDD<String,String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator){
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");
        if (_parameter.endsWith("\\|")){
            _parameter = _parameter.substring(0,_parameter.length()-1);
        }
        final String parameter = _parameter;

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                String aggrInfo = tuple._2;
                //判断年龄是否在task指定参数范围内
                if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE,
                        Constants.PARAM_END_AGE)) {
                    return false;
                }
                //根据职业判断
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                        parameter, Constants.PARAM_PROFESSIONALS)) {
                    return false;
                }
                //根据城市判断
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                        parameter, Constants.PARAM_CITIES)) {
                    return false;
                }
                //根据关键字进行判断
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                        parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }
                //根据点击商品品类进行判断
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                        parameter, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }
                //该记录通过筛选，就要进行统计
                Long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_VISIT_LENGTH));
                Long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_STEP_LENGTH));

                calculateVisitLength(visitLength);
                calculateStepLength(stepLength);


                return true;
            }
            /**
             * 计算访问时长范围
             * @param visitLength
             */
            private void calculateVisitLength(long visitLength) {
                if(visitLength >=1 && visitLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if(visitLength >=4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if(visitLength >=7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if(visitLength >=10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if(visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if(visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if(visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if(visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if(visitLength > 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }

            /**
             * 计算访问步长范围
             * @param stepLength
             */
            private void calculateStepLength(long stepLength) {
                if(stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if(stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if(stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if(stepLength >= 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if(stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if(stepLength > 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }
        });
        return  filteredSessionid2AggrInfoRDD;
    }
}
