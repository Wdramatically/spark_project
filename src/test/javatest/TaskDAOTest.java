package javatest;

import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.Task;

/**
 * 任务管理DAO测试类
 * @author Administrator
 *
 */
public class TaskDAOTest {
	
	public static void main(String[] args) {
		ITaskDAO taskDao = DAOFactory.getTaskDAO();
		Task task = taskDao.findById(2);
		System.out.println(task.getTaskName());
	}
	
}
