// imports
/* BerkelyDB classes */
import java.io.File;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class MyInterpreter {
	
	private static MyInterpreter _instance;
	
	public Environment myDBEnv;
	public Database tableListDB;

	private MyInterpreter() {
	    // Open Database Environment or if not exists, create one.
	    EnvironmentConfig envConfig = new EnvironmentConfig();
	    envConfig.setAllowCreate(true);
	    myDBEnv = new Environment(new File("db/"), envConfig);
	    
	    // Open Table List Database or if not exists, create one.
	    DatabaseConfig dbConfig = new DatabaseConfig();
	    dbConfig.setAllowCreate(true);
	    tableListDB = myDBEnv.openDatabase(null, "TableList", dbConfig);
	    
	}
	 
	public static MyInterpreter getInstance() {
		if(_instance == null)
			_instance = new MyInterpreter();
		return _instance;
	}
	
	

}
