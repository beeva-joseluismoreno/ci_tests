package com.bbva.kbdp.utils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.hadoop.hbase.client.Table;

public class ReflectionMethod extends GetCallerClassNameMethod {

	@Override
	public String getCallerClassName(int callStackDepth) {
		Field field = null;
		try {
			field = sun.reflect.Reflection.getCallerClass().getDeclaredField("table1");
			
		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		sun.reflect.Reflection.getCallerClass().getClassLoader();
//		sun.reflect.Reflection.getCallerClass().getClass().getDeclaredFields();
//		sun.reflect.Reflection.getCallerClass().getClass().getFields();
//		
		
		field.setAccessible(true);
		Type type = field.getType();
		try {
			final Table table = (Table) field.get(field);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Type type = field.getGenericType();
		//ParameterizedType pType = (ParameterizedType) type;
//		Type[] types = pType.getActualTypeArguments();
//		for (Type aType: types) {
//	        System.out.println(aType);
//	    }
		
		return sun.reflect.Reflection.getCallerClass().getName();
	}

	public Field getCallerClassField(int callStackDepth, String iafield) {
		Field field = null;
		try {
			field = sun.reflect.Reflection.getCallerClass().getDeclaredField(iafield);
			
		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Type type = field.getGenericType();
		ParameterizedType pType = (ParameterizedType) type;
		Type[] types = pType.getActualTypeArguments();
		for (Type aType: types) {
	        System.out.println(aType);
	    }
		//HTable table = (HTable)field;
		return field;
	}
	
	@Override
	public String getMethodName() {
		return "Reflection";
	}

}
