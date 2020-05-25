/**
 * FileName: DateUtils
 * Author:   hxd
 * Date:     2020/5/24 13:44
 * Description: 日期时间工具类
 */

package com.ibeifeng.sparkproject.util;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
    public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd");

    /**
     * 判断一个时间是否在另一个时间之前
     * @param time1
     * @param time2
     * @return
     */
    public static  boolean before(String time1,String time2){

        try {
            Date dateTime1 = TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);
            if (dateTime1.before(dateTime2)){
                return true;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     *判断一个时间是否在另一个时间之后
     * @param time1
     * @param time2
     * @return
     */
    public static boolean after(String time1,String time2){
        try {
            Date dateTime1 = TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);

            if (dateTime1.after(dateTime2)){
                return true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    /**
     *
     * @param time1
     * @param time2
     * @return 时间差
     */
    public static int minus(String time1,String time2){
        try {
            Date dateTime1 = TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);
            long millisecond = dateTime1.getTime()-dateTime2.getTime();
            return Integer.valueOf(String.valueOf(millisecond));
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

    /**
     *
     * @param datetime
     * @return 获取年月日小时
     */
    public static String getDateHour(String datetime){
        String date = datetime.split(" ")[0];
        String hourMinuteSecond = datetime.split(" ")[1];
        String hour = hourMinuteSecond.split(":")[0];
        return date + "_" +hour;
    }

    /**
     *
     * @return 当天日期
     */
    public static String getTodayDate(){
        return DATEKEY_FORMAT.format(new Date());
    }

    /**
     *
     * @return 昨天的日期
     */
    public  static  String getYesterdayDate(){
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR,-1);

        Date date = cal.getTime();
        return DATEKEY_FORMAT.format(date);
    }

    /**
     * 格式化日期（yyyy-MM-dd）
     * @param date
     * @return
     */
    public static  String formatDate(Date date){
        return DATE_FORMAT.format(date);
    }

    /**
     * 格式化时间（yyyy-MM-dd HH:mm:ss）
     * @param date
     * @return
     */
    public static  String formatTime(Date date){
        return TIME_FORMAT.format(date);
    }

    /**
     * 解析时间字符串
     * @param time
     * @return
     */
    public static Date parseTime(String time){
        try {
            return TIME_FORMAT.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 格式化日期key
     * @param date
     * @return
     */
    public static String formatDateKey(Date date){
        return DATEKEY_FORMAT.format(date);
    }

    /**
     * 格式化日期key
     * @param datekey
     * @return
     */
    public static Date parseDateKey(String datekey){
        try {
            return  DATEKEY_FORMAT.parse(datekey);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 格式化时间保留到分钟级别
     * @param date
     * @return
     */
    public static String formatTimeMinute(Date date){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        return sdf.format(date);
    }
}
