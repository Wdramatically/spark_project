/**
 * FileName: CategorySortKey
 * Author:   86155
 * Date:     2020/5/28 15:08
 * Description:
 */

package com.ibeifeng.sparkproject.spark.session;

import scala.Serializable;
import scala.math.Ordered;

public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {
    private Long clickCount;
    private Long orderCount;
    private Long payCount;

    public CategorySortKey(Long clickCount, Long orderCount, Long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public Long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
    }

    public Long getPayCount() {
        return payCount;
    }

    public void setPayCount(Long payCount) {
        this.payCount = payCount;
    }

    @Override
    public int compare(CategorySortKey other) {
        if (clickCount - other.clickCount != 0){
            return (int) (clickCount - other.clickCount);
        }else if(orderCount - other.orderCount != 0){
            return (int) (orderCount - other.orderCount);
        }else if(payCount - other.payCount != 0){
            return (int) (payCount - other.payCount);
        }
        return 0;
    }

    @Override
    public boolean $less(CategorySortKey other) {
        if(clickCount < other.clickCount){
            return true;
        }else if(clickCount == other.clickCount && orderCount < other.orderCount){
            return true;
        }else if(clickCount == other.clickCount && orderCount == other.orderCount && payCount < other.payCount){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey other) {
        if(clickCount > other.clickCount){
            return true;
        }else if(clickCount == other.clickCount && orderCount > other.orderCount){
            return true;
        }else if(clickCount == other.clickCount && orderCount == other.orderCount && payCount > other.payCount){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey other) {
        if($less(other)){
            return true;
        }else if(clickCount == other.clickCount && orderCount == other.orderCount && payCount == other.payCount){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey other) {
        if($greater(other)){
            return true;
        }else if(clickCount == other.clickCount && orderCount == other.orderCount && payCount == other.payCount){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(CategorySortKey other) {
        return compare(other);
    }
}
