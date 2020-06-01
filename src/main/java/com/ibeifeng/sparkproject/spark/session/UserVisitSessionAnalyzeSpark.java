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
import com.ibeifeng.sparkproject.dao.*;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.*;
import com.ibeifeng.sparkproject.util.*;
import groovy.lang.Tuple;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;
import com.google.common.base.Optional;

import java.util.*;


public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());
        sc.setLogLevel("WARN");
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
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
        JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);

        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = aggregateBySession(actionRDD, sqlContext);
        // 重构，同时进行过滤和统计

        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2FullAggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        JavaPairRDD<String, Row> session2detailRDD = getSession2detailRDD(filteredSessionid2AggrInfoRDD, sessionid2ActionRDD);

        randomExtractSession(task.getTaskid(),filteredSessionid2AggrInfoRDD,sessionid2ActionRDD);
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),task.getTaskid());

        List<Tuple2<CategorySortKey, String>> top10Category = getTop10Category(task_id, session2detailRDD);
        getTop10Session(sc,top10Category,task_id,session2detailRDD);

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



    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        JavaPairRDD<String, Row> sessionid2actionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {

                return new Tuple2<>(row.getString(2), row);
            }
        });
        return sessionid2actionRDD;

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
                   //System.out.println("此次解析的日期："+ row.getString(4));
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
                   /*System.out.println("stepLength:"+stepLength);
                   System.out.println("startTime:" + startTime);
                   System.out.println("endTime:" + endTime);*/
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



    /**
     * 过滤session数据，并进行聚合统计
     * @param sessionid2AggrInfoRDD
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator) {
        // 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
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

        if(_parameter.endsWith("|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(

                new Function<Tuple2<String,String>, Boolean>() {

                    private static final long serialVersionUID = 1L;
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        // 首先，从tuple中，获取聚合数据
                        String aggrInfo = tuple._2;
                        // 接着，依次按照筛选条件进行过滤
                        // 按照年龄范围进行过滤（startAge、endAge）
                        if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }
                        // 按照职业范围进行过滤（professionals）
                        // 互联网,IT,软件
                        // 互联网
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }
                        // 按照城市范围进行过滤（cities）
                        // 北京,上海,广州,深圳
                        // 成都
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                                parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }
                        // 按照性别进行过滤
                        if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                                parameter, Constants.PARAM_SEX)) {
                            return false;
                        }
                        // 按照搜索词进行过滤
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                                parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }
                        // 按照点击品类id进行过滤
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                                parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }
                        // 如果经过了之前的多个过滤条件之后，程序能够走到这里
                        // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
                        // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
                        // 进行相应的累加计数

                        // 主要走到这一步，那么就是需要计数的session
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        // 计算出session的访问时长和访问步长的范围，并进行相应的累加
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);

                        return true;
                    }

                    /*
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

                    /*
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
        return filteredSessionid2AggrInfoRDD;
    }

    public static JavaPairRDD<String,Row> getSession2detailRDD(JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
                         JavaPairRDD<String, Row> sessionid2ActionRDD){
        JavaPairRDD<String, Row> sessionid2detailRDD = filteredSessionid2AggrInfoRDD.join(sessionid2ActionRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                return new Tuple2<>(tuple._1, tuple._2._2);
            }
        });
        return sessionid2detailRDD;
    }


    private static void randomExtractSession(final long taskid,
                                             final JavaPairRDD<String, String> sessionid2AggrInfoRDD,
                                             final  JavaPairRDD<String,Row> sessionid2ActionRDD) {

        //将<sessionid,info>转换为<date_hour,info>格式
        JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> Tuple) throws Exception {
                String aggrInfo = Tuple._2;

                String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
                String dateHour = DateUtils.getDateHour(startTime);

                return new Tuple2<String, String>(dateHour, aggrInfo);
            }
        });
        //将每天每小时有多少条数据计算出来，放在map中
        Map<String, Object> sessionCount = time2sessionidRDD.countByKey();
        //将date信息提取出来
        Map<String,Map<String,Long>> dateHourCount = new HashMap<>();
        for (Map.Entry<String,Object> entry : sessionCount.entrySet() ){
            String key = entry.getKey();
            String date = key.split("_")[0];
            String hour = key.split("_")[1];
            Long count = Long.valueOf(String.valueOf(entry.getValue()));
            Map<String,Long> hourCount = dateHourCount.get(date);
            //判断这一天是否有map数据
            if(hourCount == null){
                hourCount = new HashMap<String, Long>();
                hourCount.put(hour,count);
                dateHourCount.put(date,hourCount);
            }else {
                hourCount.put(hour, count);
            }
        }

        //总共取100条，计算得出每天应该取多少条
        int preDayCount = (int)100 / dateHourCount.size();
        //获取date，hour，随机生成的索引list
        Map<String,Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();

        for (Map.Entry<String,Map<String,Long>> entry : dateHourCount.entrySet()){
            String date = entry.getKey();

            Map<String,Long> hourCountMap = entry.getValue();
            Random random = new Random();
            //一天拥有的session总数，用于计算每小时占当天数据量的比例
            long dateSessionCount = 0L;
            for (long hourSessionCount : hourCountMap.values() ){
                dateSessionCount += hourSessionCount;
            }
            //判断当天是否已经有对应map数据，没有则创建
            Map<String,List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null){
                hourExtractMap = new HashMap<>();
                dateHourExtractMap.put(date,hourExtractMap);
            }

            for (Map.Entry<String,Long> hourCountEntry :hourCountMap.entrySet()){
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                //这个小时应取多少数据
                int hourExtractNumber = (int)(((double)count / (double)dateSessionCount) * preDayCount);

                if(hourExtractNumber > count){
                    hourExtractNumber = (int)count;
                }

                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null){
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour,extractIndexList);
                }
                for(int i =0; i<hourExtractNumber;i++){
                    int extractIndex = random.nextInt((int)count);
                    while(extractIndexList.contains(extractIndex)){
                        extractIndex = random.nextInt((int)count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }

            //根据索引获取数据明细，并插入到数据库中
            JavaPairRDD<String, Iterable<String>> time2sessionRDD = time2sessionidRDD.groupByKey();
            JavaPairRDD<String, String> extractSessionidRDD = time2sessionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
                @Override
                public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> Tuple) throws Exception {
                    String dateHour = Tuple._1;
                    String date = dateHour.split("_")[0];
                    String hour = dateHour.split("_")[1];
                    Iterator<String> aggrInfo = Tuple._2.iterator();
                    List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
                    List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();
                    int index = 0;
                    while (aggrInfo.hasNext()) {
                        String info = aggrInfo.next();
                        if (extractIndexList.contains(index)) {
                            String sessionid = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_SESSION_ID);
                            SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                            String startTime = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_START_TIME);
                            String searchKeywords = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_SEARCH_KEYWORDS);
                            String clickCategoryIds = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS);

                            sessionRandomExtract.setTaskid(taskid);
                            sessionRandomExtract.setSessionid(sessionid);
                            sessionRandomExtract.setStartTime(startTime);
                            sessionRandomExtract.setSearchKeywords(searchKeywords);
                            sessionRandomExtract.setClickCategoryIds(clickCategoryIds);

                            ISessionRandomExtractDAO iSessionRandomExtractDAO = DAOFactory.getISessionRandomExtractDAO();
                            iSessionRandomExtractDAO.insert(sessionRandomExtract);

                            extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));

                        }
                        index++;
                    }
                    return extractSessionids;
                }
            });

            JavaPairRDD<String, Tuple2<String, Row>> sessionid2sessionidInfo =
                    extractSessionidRDD.join(sessionid2ActionRDD);
            sessionid2sessionidInfo.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
                @Override
                public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                    String sessionid = tuple._1;
                    long task_id = taskid;
                    Row info = tuple._2._2;
                    SessionDetail sessionDetail = new SessionDetail();

                    sessionDetail.setTaskid(task_id);
                    sessionDetail.setUserid(info.getLong(1));
                    sessionDetail.setSessionid(sessionid);
                    sessionDetail.setPageid(info.getLong(3));
                    sessionDetail.setActionTime(info.getString(4));
                    sessionDetail.setSearchKeyword(info.getString(5));
                    sessionDetail.setClickCategoryId(info.getLong(6));
                    sessionDetail.setClickProductId(info.getLong(7));
                    sessionDetail.setOrderCategoryIds(info.getString(8));
                    sessionDetail.setOrderProductIds(info.getString(9));
                    sessionDetail.setPayCategoryIds(info.getString(10));
                    sessionDetail.setPayProductIds(info.getString(11));

                    ISessionDetailDAO iSessionDetailDAO = DAOFactory.getISessionDetailDAO();
                    iSessionDetailDAO.insert(sessionDetail);

                }
            });


        }
    }
    /**
     * 计算各session范围占比，并写入MySQL
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {

        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils .getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double)step_length_60 / (double)session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(Long taskid,
                                         JavaPairRDD<String, Row> sessionid2detailRDD) {

        //获取每条数据中的click、order、pay品类ID
        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                Long clickCategoryId = row.getLong(6);
                if(clickCategoryId != null){
                    list.add(new Tuple2<>(clickCategoryId,clickCategoryId));
                }
                String orderCategoryIds = row.getString(8);
                if (orderCategoryIds != null){
                    String[] orderProductIdArray = orderCategoryIds.split("'");
                    for (String orderProductId:orderProductIdArray) {
                        list.add(new Tuple2<>(Long.valueOf(orderProductId),Long.valueOf(orderProductId)));
                    }
                }
                String payCategoryIds = row.getString(10);
                if (payCategoryIds != null){
                    String[] payProductIdArray = payCategoryIds.split("'");
                    for (String payProductId:payProductIdArray) {
                        list.add(new Tuple2<>(Long.valueOf(payProductId),Long.valueOf(payProductId)));
                    }
                }
                return list;
            }
        });

        categoryidRDD = categoryidRDD.distinct();
        //计算各品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2countRDD =  getClickCountRDD(sessionid2detailRDD);
        JavaPairRDD<Long, Long> orderCategoryId2countRDD =  getOrderCountRDD(sessionid2detailRDD);
        JavaPairRDD<Long, Long> payCategoryId2countRDD =  getPayCountRDD(sessionid2detailRDD);

        JavaPairRDD<Long,String> categoryid2countRDD = joinCategoryAndData(categoryidRDD,clickCategoryId2countRDD,orderCategoryId2countRDD,payCategoryId2countRDD);

        //自定义排序key

        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple2) throws Exception {
                String countInfo = tuple2._2;
                Long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                Long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                Long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));

                CategorySortKey categorySortKey = new CategorySortKey(clickCount, orderCount, payCount);
                return new Tuple2<>(categorySortKey, countInfo);
            }
        });
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);
        List<Tuple2<CategorySortKey, String>> top10category = sortedCategoryCountRDD.take(10);
        for (Tuple2<CategorySortKey,String> tmpList : top10category ){
            String info = tmpList._2;

            Top10Category top10Category = new Top10Category();
            Long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_CATEGORY_ID));
            Long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_CLICK_COUNT));
            Long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_ORDER_COUNT));
            Long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_PAY_COUNT));

            top10Category.setTaskid(taskid);
            top10Category.setCategoryid(categoryid);
            top10Category.setClickCount(clickCount);
            top10Category.setOrderCount(orderCount);
            top10Category.setPayCount(payCount);

            ITop10CategoryDAO iTop10CategoryDAO = DAOFactory.getITop10CategoryDAO();
            iTop10CategoryDAO.insert(top10Category);
        }
        return top10category;

    }
    private static JavaPairRDD<Long, Long> getClickCountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        //过滤掉空值
        JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                Row row = v1._2;
                return String.valueOf(row.getLong(6)) != null ? true : false;
            }
        });
        //转换成<id,1>的形式方便统计
        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                return new Tuple2<Long, Long>(tuple._2.getLong(6), 1L);
            }
        });
        JavaPairRDD<Long, Long> clickCategoryId2countRDD = clickCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return clickCategoryId2countRDD;
    }
    private static JavaPairRDD<Long, Long> getOrderCountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                Row row = v1._2;
                return row.getString(8) != null ? true : false;
            }
        });
        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
                String orderCategoryIds = tuple2._2.getString(8);
                String[] orderCategoryIdArray = orderCategoryIds.split(",");
                List<Tuple2<Long, Long>> orderCategoryList = new ArrayList<>();
                for (String orderCategroyId : orderCategoryIdArray) {
                    orderCategoryList.add(new Tuple2<>(Long.valueOf(orderCategroyId), 1L));
                }
                return orderCategoryList;
            }
        });
        JavaPairRDD<Long, Long> orderCategoryId2countRDD = orderCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return orderCategoryId2countRDD;
    }
    private static JavaPairRDD<Long, Long> getPayCountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {

        JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                Row row = v1._2;
                return row.getString(10) != null ? true : false;
            }
        });


        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
                String payCategoryIds = tuple2._2.getString(10);
                String[] payCategoryIdArray = payCategoryIds.split(",");
                List<Tuple2<Long, Long>> payCategoryList = new ArrayList<>();
                for (String payCategroyId : payCategoryIdArray) {
                    payCategoryList.add(new Tuple2<>(Long.valueOf(payCategroyId), 1L));
                }
                return payCategoryList;
            }
        });


        JavaPairRDD<Long, Long> payCategoryId2countRDD = payCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
    return payCategoryId2countRDD;
    }

    private static JavaPairRDD<Long,String> joinCategoryAndData(JavaPairRDD<Long, Long> categoryidRDD, JavaPairRDD<Long, Long> clickCategoryId2countRDD, JavaPairRDD<Long, Long> orderCategoryId2countRDD, JavaPairRDD<Long, Long> payCategoryId2countRDD) {
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinPairRDD =
                categoryidRDD.leftOuterJoin(clickCategoryId2countRDD);
        JavaPairRDD<Long, String> tmpMapPairRDD = tmpJoinPairRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {
                Long categoryId = tuple._1;
                Optional<Long> option = tuple._2._2;
                Long clickCount = 0L;
                if (option.isPresent()) {
                    clickCount = option.get();
                }
                String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" +
                        Constants.FIELD_CLICK_COUNT + "=" + clickCount + "|";
                return new Tuple2<>(categoryId, value);
            }
        });
        tmpMapPairRDD = tmpMapPairRDD.leftOuterJoin(orderCategoryId2countRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                Long categoryId = tuple._1;
                String value = tuple._2._1;
                Optional<Long> option = tuple._2._2;
                Long orderCount = 0L;
                if (option.isPresent()) {
                    orderCount = option.get();
                }
                value = value  +"|" +
                        Constants.FIELD_ORDER_COUNT + "=" + orderCount + "|";
                return new Tuple2<>(categoryId, value);
            }
        });
        tmpMapPairRDD = tmpMapPairRDD.leftOuterJoin(payCategoryId2countRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                Long categoryId = tuple._1;
                String value = tuple._2._1;
                Optional<Long> option = tuple._2._2;
                Long payCount = 0L;
                if (option.isPresent()) {
                    payCount = option.get();
                }
                value = value + "|" +
                        Constants.FIELD_PAY_COUNT + "=" + payCount + "|";
                return new Tuple2<>(categoryId, value);
            }
        });
    return  tmpMapPairRDD;
    }
    private static void getTop10Session(JavaSparkContext sc,List<Tuple2<CategorySortKey, String>> top10Categorys, Long task_id, JavaPairRDD<String, Row> session2detailRDD) {
        /**
         * 第一步：将top10热门品类的Id生成一份RDD
         */
        List<Tuple2<Long,Long>> top10CategoryIdList = new ArrayList<>();
        for (Tuple2<CategorySortKey, String> top10Category : top10Categorys){
            String info = top10Category._2;
            Long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<>(categoryId,categoryId));
        }
        JavaPairRDD<Long, Long> top10CategoryId = sc.parallelizePairs(top10CategoryIdList);
        JavaPairRDD<String, Iterable<Row>> sessionid2detailInfo = session2detailRDD.groupByKey();

        JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailInfo.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionid = tuple._1;
                Iterator<Row> iterator = tuple._2.iterator();
                Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();

                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (row.get(6) != null) {
                        Long categoryId = row.getLong(6);
                        Long count = categoryCountMap.get(categoryId);
                        if (count == null) {
                            count = 0L;
                        }
                        count++;
                        categoryCountMap.put(categoryId, count);
                    }
                }
                List<Tuple2<Long, String>> list = new ArrayList<>();
                for (Map.Entry<Long, Long> entry : categoryCountMap.entrySet()) {
                    String value = sessionid + "," + entry.getValue();
                    list.add(new Tuple2<Long, String>(entry.getKey(), value));
                }

                return list;
            }
        });
        //获取到top10热门品类对应的session以及每个session的点击次数
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryId.join(categoryid2sessionCountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
                return new Tuple2<Long, String>(tuple._1, tuple._2._2);
            }
        });
        JavaPairRDD<Long, Iterable<String>> top10Category2sessionids = top10CategorySessionCountRDD.groupByKey();

        JavaPairRDD<String, String> top10SessionRDD = top10Category2sessionids.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
                Long categoryId = tuple._1;
                Iterator<String> sessionIdCount = tuple._2.iterator();
                String[] sessionIds = new String[5];
                while (sessionIdCount.hasNext()) {
                    String tmp = sessionIdCount.next();
                    Long count = Long.valueOf(tmp.split(",")[1]);
                    for (int i = 0; i < sessionIds.length; i++) {
                        if (sessionIds[i] == null) {
                            sessionIds[i] = tmp;
                            break;
                        } else {
                            long _count = Long.valueOf(sessionIds[i].split(",")[1]);
                            if (count > _count) {
                                for (int j = 4; j > i; j--) {
                                    sessionIds[j] = sessionIds[j - 1];
                                }
                                sessionIds[i] = tmp;
                                break;
                            }
                        }
                    }
                }
                List<Tuple2<String, String>> list = new ArrayList<>();

                for (String tmpString : sessionIds) {
                    Top10Session top10Session = new Top10Session();
                    String sessionID = tmpString.split(",")[0];
                    Long count = Long.valueOf(tmpString.split(",")[1]);
                    top10Session.setCategoryid(categoryId);
                    top10Session.setTaskid(task_id);
                    top10Session.setClickCount(count);
                    top10Session.setSessionid(sessionID);
                    ITop10SessionDAO iTop10SessionDAO = DAOFactory.getITop10SessionDAO();
                    iTop10SessionDAO.insert(top10Session);
                    list.add(new Tuple2<>(sessionID, sessionID));
                }
                return list;
            }
        });
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
                top10SessionRDD.join(session2detailRDD);
        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(task_id);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getISessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });
    }
}
