/**
 * FileName: ParamUtils
 * Author:   86155
 * Date:     2020/5/24 14:30
 * Description:
 */

package com.ibeifeng.sparkproject.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;

public class ParamUtils {
    /**
     * 从命令行中获取任务id
     * @param args
     * @param taskType
     * @return 任务id
     */
    public  static Long getTaskIdFromArgs(String[] args,String taskType){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            return  ConfigurationManager.getLong(taskType);
        }else{
            if (args != null && args.length > 0){
                return Long.valueOf(args[0]);
            }
        }
        return null;
    }

    /**
     * 从JSON对象中提取参数
     * @param jsonObject
     * @param field
     * @return
     */
    public static String getParam(JSONObject jsonObject,String field){
        JSONArray jsonArray = jsonObject.getJSONArray(field);
        if (jsonArray != null && jsonArray.size() > 0){
            return jsonArray.getString(0);
        }
        return  null;
    }
}
