import java.io.Serializable;

public class DBType implements Serializable {
	private static final long serialVersionUID = -8524409273436817358L;

	public enum DBTypeSpecifier { DB_INT, DB_CHAR, DB_DATE, DB_NULL };
	
	public DBTypeSpecifier type;
	public int length; // only used for DB_CHAR
	
	public DBType(DBType other) { // Copy Instructor
		type = other.type;
		length = other.length;
	}

	public DBType(String typeName, int value) {
		this(typeName);
		if(typeName.equals("char")) {
			length = value;
		}
		else
			throw new IllegalArgumentException();
	}
	
	public DBType(String typeName)
	{
		length = -1; // Not used in int and date
		
		if(typeName.equals("int")) 
			type = DBTypeSpecifier.DB_INT;
		else if(typeName.equals("date")) 
			type = DBTypeSpecifier.DB_DATE;
		else if(typeName.equals("char"))
			type = DBTypeSpecifier.DB_CHAR;
		else
			throw new IllegalArgumentException();
	}
	
	@Override
	public String toString(){
		switch(type)
		{
			case DB_INT:
				return "int";
			case DB_DATE:
				return "date";
			case DB_CHAR:
				return "char(" + Integer.toString(length) + ")";
			default:
				return "TypeError";
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DBType other = (DBType) obj;
		if (type != other.type)
			return false;
		if (type == DBTypeSpecifier.DB_CHAR && length != other.length) // length only matters when this is char
			return false;
		return true;
	}
}
