import java.io.Serializable;
import java.util.ArrayList;

public class DBValue implements Serializable {
	private static final long serialVersionUID = -1796410168769767330L;
	
	public DBType.DBTypeSpecifier valueType;
	public int intVal;
	public String charVal;
	public ArrayList<Integer> dateVal;

	public DBValue(int value) {
		valueType = DBType.DBTypeSpecifier.DB_INT;
		intVal = value;
		charVal = null;
		dateVal = null;
	}
	
	public DBValue(String value) {
		valueType = DBType.DBTypeSpecifier.DB_CHAR;
		charVal = value;
		intVal = 0;
		dateVal = null;
	}
	
	public DBValue(int year, int month, int date) {
		valueType = DBType.DBTypeSpecifier.DB_DATE;
		dateVal = new ArrayList<Integer>();
		dateVal.add(year);
		dateVal.add(month);
		dateVal.add(date);
		intVal = 0;
		charVal = null;
	}
	
	public DBValue() {
		valueType = DBType.DBTypeSpecifier.DB_NULL;
		intVal = 0;
		charVal = null;
		dateVal = null;
	}
	
	public void trimChar(int size) {
		if(valueType != DBType.DBTypeSpecifier.DB_CHAR) return;
		
		if(charVal.length() > size)
			charVal = charVal.substring(0, size);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((charVal == null) ? 0 : charVal.hashCode());
		result = prime * result + ((dateVal == null) ? 0 : dateVal.hashCode());
		result = prime * result + intVal;
		result = prime * result
				+ ((valueType == null) ? 0 : valueType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DBValue other = (DBValue) obj;
		if (charVal == null) {
			if (other.charVal != null)
				return false;
		} else if (!charVal.equals(other.charVal))
			return false;
		if (dateVal == null) {
			if (other.dateVal != null)
				return false;
		} else if (!dateVal.equals(other.dateVal))
			return false;
		if (intVal != other.intVal)
			return false;
		if (valueType != other.valueType)
			return false;
		return true;
	}

	@Override
	public String toString() {
		if(valueType == DBType.DBTypeSpecifier.DB_INT)
			return Integer.toString(intVal);
		else if(valueType == DBType.DBTypeSpecifier.DB_CHAR)
			return charVal;
		else if(valueType == DBType.DBTypeSpecifier.DB_DATE)
			return String.format("%04d-%02d-%02d", dateVal.get(0), dateVal.get(1), dateVal.get(2));
		else
			return "NULL";
	}
	
	public boolean isNull() {
		if(valueType == DBType.DBTypeSpecifier.DB_NULL)
			return true;
		else
			return false;
	}
	
	public static ThreeValuedLogic Compare(DBValue a, CompOperator op, DBValue b) throws WhereIncomparableError
	{
		if(a.isNull() || b.isNull()) 
			return ThreeValuedLogic.TVL_UNKNOWN;
		
		if(a.valueType != b.valueType)
			throw new WhereIncomparableError();
		
		boolean result = false;
		switch(a.valueType) {
		case DB_INT:
			result = a._intCompare(op, b);
			break;
		case DB_CHAR:
			result = a._charCompare(op, b);
			break;
		case DB_DATE:
			result = a._dateCompare(op, b);
			break;
		default:
			break;
		}
		
		return (result) ? ThreeValuedLogic.TVL_TRUE : ThreeValuedLogic.TVL_FALSE;
	}
	
	private boolean _intCompare(CompOperator op, DBValue other) {
		switch(op) {
		case OP_GT:
			return this.intVal > other.intVal;
		case OP_LT:
			return this.intVal < other.intVal;
		case OP_GE:
			return this.intVal >= other.intVal;
		case OP_LE:
			return this.intVal <= other.intVal;
		case OP_EQ:
			return this.intVal == other.intVal;
		case OP_NEQ:
			return this.intVal != other.intVal;
		default:
			return false;
		}
	}
	
	private boolean _charCompare(CompOperator op, DBValue other) {
		switch(op) {
		case OP_GT:
			return this.charVal.compareTo(other.charVal) > 0;
		case OP_LT:
			return this.charVal.compareTo(other.charVal) < 0;
		case OP_GE:
			return this.charVal.compareTo(other.charVal) >= 0;
		case OP_LE:
			return this.charVal.compareTo(other.charVal) <= 0;
		case OP_EQ:
			return this.charVal.compareTo(other.charVal) == 0;
		case OP_NEQ:
			return this.charVal.compareTo(other.charVal) != 0;
		default:
			return false;
		}
	}
	
	private boolean _dateCompare(CompOperator op, DBValue other) {
		boolean result = false;
		
		switch(op) {
		case OP_GT:
			result = this.dateVal.get(0).intValue() > other.dateVal.get(0).intValue();
			if(this.dateVal.get(0).intValue() == other.dateVal.get(0).intValue()) {
				result = this.dateVal.get(1).intValue() > other.dateVal.get(1).intValue();
				if(this.dateVal.get(1).intValue() == other.dateVal.get(1).intValue())
					result = this.dateVal.get(2).intValue() > other.dateVal.get(2).intValue();
			}
			break;
		case OP_LT:
			result = this.dateVal.get(0).intValue() < other.dateVal.get(0).intValue();
			if(this.dateVal.get(0).intValue() == other.dateVal.get(0).intValue()) {
				result = this.dateVal.get(1).intValue() < other.dateVal.get(1).intValue();
				if(this.dateVal.get(1).intValue() == other.dateVal.get(1).intValue())
					result = this.dateVal.get(2).intValue() < other.dateVal.get(2).intValue();
			}
			break;
		case OP_GE:
			result = this.dateVal.get(0).intValue() > other.dateVal.get(0).intValue();
			if(this.dateVal.get(0).intValue() == other.dateVal.get(0).intValue()) {
				result = this.dateVal.get(1).intValue() > other.dateVal.get(1).intValue();
				if(this.dateVal.get(1).intValue() == other.dateVal.get(1).intValue())
					result = this.dateVal.get(2).intValue() >= other.dateVal.get(2).intValue();
			}
			break;
		case OP_LE:
			result = this.dateVal.get(0).intValue() < other.dateVal.get(0).intValue();
			if(this.dateVal.get(0).intValue() == other.dateVal.get(0).intValue()) {
				result = this.dateVal.get(1).intValue() < other.dateVal.get(1).intValue();
				if(this.dateVal.get(1).intValue() == other.dateVal.get(1).intValue())
					result = this.dateVal.get(2).intValue() <= other.dateVal.get(2).intValue();
			}
			break;
		case OP_EQ:
			result = this.dateVal.get(0).intValue() == other.dateVal.get(0).intValue();
			result = result && (this.dateVal.get(1).intValue() == other.dateVal.get(1).intValue());
			result = result && (this.dateVal.get(2).intValue() == other.dateVal.get(2).intValue());
			break;
		case OP_NEQ:
			result = this.dateVal.get(0).intValue() != other.dateVal.get(0).intValue();
			result = result || (this.dateVal.get(1).intValue() != other.dateVal.get(1).intValue());
			result = result || (this.dateVal.get(2).intValue() != other.dateVal.get(2).intValue());
			break;
		default:
			break;
		}
		
		return result;
	}
	
}

enum CompOperator { OP_GT, OP_LT, OP_GE, OP_LE, OP_EQ, OP_NEQ };
enum ThreeValuedLogic { TVL_TRUE, TVL_FALSE, TVL_UNKNOWN };
