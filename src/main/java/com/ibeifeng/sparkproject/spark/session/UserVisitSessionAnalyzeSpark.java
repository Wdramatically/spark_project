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
import com.ibeifeng.sparkproject.dao.ISessionAggrStatDAO;
import com.ibeifeng.sparkproject.dao.ISessionRandomExtractDAO;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.SessionAggrStat;
import com.ibeifeng.sparkproject.domain.SessionRandomExtract;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;
import com.ibeifeng.sparkproject.util.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.*;


public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local[1]");
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
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = aggregateBySession(actionRDD, sqlContext);
        // 重构，同时进行过滤和统计

        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2FullAggrInfoRDD, taskParam, sessionAggrStatAccumulator);


        randomExtractSession(task.getTaskid(),filteredSessionid2AggrInfoRDD);
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),task.getTaskid());

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
                "from user_visit_action " +
                "where date >= '" + startDate +"' " +
                "and date <= '" + stopDate + "'";
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


    private static void randomExtractSession(long taskid,JavaPairRDD<String, String> sessionid2AggrInfoRDD) {
        JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> Tuple) throws Exception {
                String aggrInfo = Tuple._2;

                String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
                String dateHour = DateUtils.getDateHour(startTime);

                return new Tuple2<String, String>(dateHour, aggrInfo);
            }
        });

        Map<String, Object> sessionCount = time2sessionidRDD.countByKey();
        Map<String,Map<String,Long>> dateHourCount = new HashMap<>();
        for (Map.Entry<String,Object> entry : sessionCount.entrySet() ){
            String key = entry.getKey();
            String date = key.split("_")[0];
            String hour = key.split("_")[1];
            Long count = Long.valueOf(String.valueOf(entry.getValue()));
            Map<String,Long> hourCount = dateHourCount.get(date);
            if(hourCount == null){
                hourCount = new HashMap<String, Long>();
                hourCount.put(hour,count);
                dateHourCount.put(date,hourCount);
            }else {
                hourCount.put(hour, count);
            }
        }

        int preDayCount = (int)100 / dateHourCount.size();

        Map<String,Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();

        for (Map.Entry<String,Map<String,Long>> entry : dateHourCount.entrySet()){
            String date = entry.getKey();

            Map<String,Long> hourCountMap = entry.getValue();
            Random random = new Random();
            long dateSessionCount = 0L;
            for (long hourSessionCount : hourCountMap.values() ){
                dateSessionCount += hourSessionCount;
            }

            Map<String,List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null){
                hourExtractMap = new HashMap<>();
                dateHourExtractMap.put(date,hourExtractMap);
            }
            for (Map.Entry<String,Long> hourCountEntry :hourCountMap.entrySet()){
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

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

            JavaPairRDD<String, Iterable<String>> time2sessionRDD = time2sessionidRDD.groupByKey();
            time2sessionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
                @Override
                public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> Tuple) throws Exception {
                    String dateHour = Tuple._1;
                    String date = dateHour.split("_")[0];
                    String hour = dateHour.split("_")[1];
                    Iterator<String> aggrInfo = Tuple._2.iterator();
                    List<Integer> extractIndexList= dateHourExtractMap.get(date).get(hour);
                    List<Tuple2<String,String>> extractSessionids = new ArrayList<Tuple2<String,String>>();
                    int index = 0;
                    while(aggrInfo.hasNext()){
                        String info = aggrInfo.next();
                        if(extractIndexList.contains(index)){
                            String sessionid = StringUtils.getFieldFromConcatString(info,"\\|",Constants.FIELD_SESSION_ID);
                            SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                            String startTime = StringUtils.getFieldFromConcatString(info,"\\|",Constants.FIELD_START_TIME);
                            String searchKeywords = StringUtils.getFieldFromConcatString(info,"\\|",Constants.FIELD_SEARCH_KEYWORDS);
                            String clickCategoryIds = StringUtils.getFieldFromConcatString(info,"\\|",Constants.FIELD_CLICK_CATEGORY_IDS);

                            sessionRandomExtract.setTaskid(taskid);
                            sessionRandomExtract.setSessionid(sessionid);
                            sessionRandomExtract.setStartTime(startTime);
                            sessionRandomExtract.setSearchKeywords(searchKeywords);
                            sessionRandomExtract.setClickCategoryIds(clickCategoryIds);

                            ISessionRandomExtractDAO iSessionRandomExtractDAO = DAOFactory.getISessionRandomExtractDAO();
                            iSessionRandomExtractDAO.insert(sessionRandomExtract);

                            extractSessionids.add(new Tuple2<String,String>(sessionid,sessionid));

                        }
                        index ++;
                    }
                    return extractSessionids;
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

        System.out.println(Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT)));
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
}
