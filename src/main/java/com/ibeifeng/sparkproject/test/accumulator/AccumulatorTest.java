/**
 * FileName: AccumulatorTest
 * Author:   86155
 * Date:     2020/5/26 19:16
 * Description:
 */

package com.ibeifeng.sparkproject.test.accumulator;

import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.util.StringUtils;
import org.apache.spark.AccumulatorParam;


public class AccumulatorTest implements AccumulatorParam<String> {

    private static final long serialVersionUID = 6311074555136039130L;

    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0";
    }

    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1, r2);
    }


    public String add(String v1,String v2){
        if(v2 == null){
            return v1;
        }
        String oldvalue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (oldvalue != null){
            int newValue = Integer.valueOf(oldvalue) + 1;
            return StringUtils.setFieldInConcatString(v1,"\\|",v2,String.valueOf(newValue));
        }
        return  StringUtils.setFieldInConcatString(v1,"\\|",v2,String.valueOf(1));
    }


}
