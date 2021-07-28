package databaseconnectortest.test.callablestatement;

public class Queries {

	public final static String TAKE_LONG_RETURN_LONG =
			"declare\r\n" + 
			"  l_long number(20,0) := :1;\r\n" + 
			"begin\r\n" + 
			"  :2 := l_long;\r\n" + 
			"end;";

	public final static String TAKE_LONG_RETURN_CHAR =
			"declare\r\n" + 
			"  l_long number(20,0) := :1;\r\n" + 
			"begin\r\n" + 
			"  :2 := TO_CHAR(l_long);\r\n" + 
			"end;";

	public final static String TAKE_ALL_TYPES_RETURN_ALL_TYPES =
			"declare\r\n" + 
			"  l_long number(20,0) := :1;\r\n" + 
			"  l_dec number(20,2) := :2;\r\n" + 
			"  l_string VARCHAR2(100) := :3;\r\n" + 
			"  l_date date := :4;\r\n" + 
			"begin\r\n" + 
			"  :5 := l_date;\r\n" + 
			"  :6 := l_string;\r\n" + 
			"  :7 := l_dec;\r\n" + 
			"  :8 := l_long;\r\n" + 
			"end;";

	public final static String RETURN_DECIMAL =
			"declare\r\n" + 
			"  l_1 number(20,2) := 5.99;\r\n" + 
			"  l_2 number(20,2) := 5;  \r\n" + 
			"  l_3 number(20,0) := 5;  \r\n" + 
			"begin\r\n" + 
			"  :1 := l_1;\r\n" + 
			"  :2 := l_2;\r\n" + 
			"  :3 := l_3;\r\n" + 
			"  :4 := 5;\r\n" + 
			"end;";

	public final static String TAKE_OBJECT_RETURN_MEMBERS =
			"declare\r\n" +
			"  l_rec NAME_AND_AGE := :1;\r\n" + 
			"begin\r\n" + 
			"  :2 := l_rec.name;\r\n" + 
			"  :3 := l_rec.age;\r\n" + 
			"end;";

	public final static String TAKE_MEMBERS_RETURN_OBJECT =
			"declare\r\n" +
			"  name VARCHAR2(100) := :1;\r\n" + 
			"  age NUMBER := :2;\r\n" + 
			"begin\r\n" + 
			"  :3 := NAME_AND_AGE(name, age);\r\n" + 
			"end;";

	public final static String TAKE_OBJECTS_RETURN_OBJECTS =
			"declare\r\n" +
			"  input1 NAME_AND_AGE := :1;\r\n" + 
			"  input2 NAME_AND_AGE := :2;\r\n" + 
			"  input3 NAME_AND_AGE := :3;\r\n" + 
			"begin\r\n" + 
			"  :4 := input3;\r\n" + 
			"  :5 := input2;\r\n" + 
			"  :6 := input1;\r\n" + 
			"end;";

	public final static String TAKE_TWO_LONGS_RETURN_LIST_OF_6 =
			"declare\r\n" + 
			"  l_val1 number(20,0) := :1;\r\n" +
			"  l_val2 number(20,0) := :2;\r\n" +
			"begin\r\n" +
			"  :3 := array_6_numbers(l_val1, l_val2, l_val1, l_val2, l_val1, l_val2);\r\n" +
			"end;";

	public final static String TAKE_DATE_RETURN_LIST_OF_1 =
			"declare\r\n" + 
			"  l_val1 date := :1;\r\n" +
			"begin\r\n" +
			"  :2 := array_1_date(l_val1);\r\n" +
			"end;";

	public final static String TAKE_DECIMAL_RETURN_LIST_OF_STRING_AND_LIST_OF_DECIMAL =
			"declare\r\n" + 
			"  l_val1 number(20,2) := :1;\r\n" +
			"begin\r\n" +
			"  :2 := array_6_numbers(l_val1, l_val1);\r\n" +
			"  :3 := array_6_strings('test', 'test', 'test');\r\n" +
			"end;";
	
	public final static String TAKE_NOTHING_RETURN_EMPTY_LIST =
			"begin\r\n" +
			"  :1 := array_6_numbers();\r\n" +
			"end;";
	
	public final static String TAKE_NOTHING_RETURN_NULL =
			"begin\r\n" +
			"  :1 := null;\r\n" +
			"end;";

	public final static String TAKE_NOTHING_RETURN_LIST_BY_NAME =
			"begin\r\n" +
			"  :result := array_6_numbers(0, 1, 2);\r\n" +
			"end;";

	public final static String TAKE_LIST_OF_LONG_RETURN_SUM =
			"declare\r\n" + 
			"  l_val1 array_6_numbers := :1;\r\n" +
			"  sum1 number(20,0) := 0;\r\n" +
			"  total integer;\r\n" +
			"begin\r\n" +
			"  total := l_val1.count;\r\n" +
			"  FOR i in 1..total LOOP\r\n" +
			"  	 sum1 := sum1 + l_val1(i);\r\n" +
			"  END LOOP;\r\n" +
			"  :2 := sum1;\r\n" +
			"end;";
	
	public final static String DIVIDE_BY_INPUT =
			"declare\r\n" +
			"  l_long number(20,0) := :1;\r\n" + 
			"begin\r\n" +
			"  :2 := 1 / l_long;\r\n" +
			"end;";
	
	public final static String INOUT_ARGUMENT_BY_NAME =
			"BEGIN :result := :in_long * 3; END;";
	
	public final static String LONG_TO_REFCURSOR =
			"declare\r\n" + 
			"  l_total number(20,0) := :1;\r\n" +
			"  c_result SYS_REFCURSOR;\r\n" +
			"begin\r\n" +
			"	OPEN c_result FOR \r\n" + 
			"		SELECT LEVEL\r\n" + 
			"		FROM DUAL\r\n" + 
			"		WHERE l_total > 0\r\n" + 
			"		CONNECT BY LEVEL <= l_total;" +
			"  :2 := c_result;\r\n" +
			"end;";

	
	public final static String LONG_TO_REFCURSOR_BY_NAME =
			"declare\r\n" + 
			"  c_result SYS_REFCURSOR;\r\n" +
			"begin\r\n" +
			"	OPEN c_result FOR \r\n" + 
			"		SELECT LEVEL\r\n" + 
			"		FROM DUAL\r\n" + 
			"		CONNECT BY LEVEL <= :l_amount;" +
			"  :result := c_result;\r\n" +
			"end;";

	public final static String REFCURSOR_MULTIPLE_COLUMNS =
			"declare\r\n" + 
			"  c_result SYS_REFCURSOR;\r\n" +
			"begin\r\n" +
			"	OPEN c_result FOR\r\n" + 
			"		SELECT NULL, LEVEL, CONCAT('NAME ', TO_CHAR(LEVEL))\r\n" + 
			"		FROM DUAL\r\n" + 
			"		CONNECT BY LEVEL <= 3;" +
			"  :1 := c_result;\r\n" +
			"end;";

	public final static String REFCURSOR_NULL_FIRST =
			"declare\r\n" + 
			"  c_result SYS_REFCURSOR;\r\n" +
			"begin\r\n" +
			"	OPEN c_result FOR\r\n" + 
			"		SELECT (CASE WHEN (LEVEL < 3) THEN NULL ELSE LEVEL END)\r\n" + 
			"		FROM DUAL\r\n" + 
			"		CONNECT BY LEVEL <= 3;" +
			"  :1 := c_result;\r\n" +
			"end;";

	public final static String ARRAY_2_OBJECTS =
			"BEGIN :1 := ARRAY_2_OBJ(NAME_AND_AGE('a', 1), NAME_AND_AGE('b', 2)); END;";
	
	public final static String ARRAY_6_ARRAYS =
			"BEGIN" +
			"	:1 := ARRAY_6_ARRAYS(" +
			"			ARRAY_6_NUMBERS(1)," +
			"			ARRAY_6_NUMBERS(2, 2)," +
			"			ARRAY_6_NUMBERS(3, 3, 3)," +
			"			ARRAY_6_NUMBERS(4, 4, 4, 4)," +
			"			ARRAY_6_NUMBERS(5, 5, 5, 5, 5)," +
			"			ARRAY_6_NUMBERS(6, 6, 6, 6, 6, 6));" +
			"END;";
	
	public final static String CREATE_TYPE_NAME_AND_AGE =
			"create or replace type NAME_AND_AGE is object (name VARCHAR2(100), age NUMBER)";
	public final static String CREATE_TYPE_ARRAY_6_NUMBERS =
			"create or replace type ARRAY_6_NUMBERS is VARRAY(6) OF number(20);";
	public final static String CREATE_TYPE_ARRAY_1_DATE =
			"create or replace type array_1_date is VARRAY(1) OF date;";
	public final static String CREATE_TYPE_ARRAY_6_STRINGS =
			"create or replace type array_6_strings is VARRAY(6) OF VARCHAR2(100);";
	public final static String CREATE_TYPE_ARRAY_2_OBJ =
			"create or replace type ARRAY_2_OBJ is VARRAY(2) OF NAME_AND_AGE;";
	public final static String CREATE_TYPE_ARRAY_6_ARRAYS =
			"create or replace type ARRAY_6_ARRAYS is VARRAY(6) OF ARRAY_6_NUMBERS;";
	public final static String CREATE_PROCEDURE_LONG_TO_LONG =
			"CREATE OR REPLACE PROCEDURE long_to_long (lval IN OUT NUMBER) AS\r\n" + 
			"   BEGIN\r\n" + 
			"   lval := lval * 2;\r\n" + 
			"END;";
	public final static String CREATE_PROCEDURE_LONG_TO_DIFFERENT_LONG =
			"CREATE OR REPLACE PROCEDURE long_to_different_long (lval IN NUMBER, result OUT NUMBER) AS\r\n" + 
			"   BEGIN\r\n" + 
			"   result := lval * 3;\r\n" + 
			"END;";
	public final static String CREATE_PROCEDURE_OBJECT_TO_OBJECT =
			"CREATE OR REPLACE PROCEDURE object_to_same_object (lval IN OUT NAME_AND_AGE) AS\r\n" + 
			"   BEGIN\r\n" + 
			"   lval.age := lval.age * 2;\r\n" + 
			"   lval.name := 'new value';\r\n" + 
			"END;";
}
