package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.SessionDetail;

/**
 * Session明细DAO接口
 * @author Administrator
 *
 */
public interface ISessionDetailDAO {

	/**
	 * 插入一条session明细数据
	 * @param sessionDetail 
	 */
	void insert(SessionDetail sessionDetail);
	
}
