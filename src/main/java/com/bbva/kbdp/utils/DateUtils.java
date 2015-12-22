package com.bbva.kbdp.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.bbva.kbdp.domain.KeyByDate;

public class DateUtils {
	
	public static long getTimeFromString(String input){ 
		
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		long ldate = 0; 

		try {

			Date date = formatter.parse(input);
			ldate = date.getTime();

		} catch (ParseException e) {
			e.printStackTrace();
		}

		return ldate;
		
	}

	public static long getTimeFromString(String input, String format){ 
		
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		long ldate = 0; 

		try {

			Date date = formatter.parse(input);
			ldate = date.getTime();

		} catch (ParseException e) {
			e.printStackTrace();
		}

		return ldate;
		
	}
	
	public static Date getDateFromString(String input){
		
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		//SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		Date date = null;

		try {

			date = formatter.parse(input);

		} catch (ParseException e) {
			e.printStackTrace();
		}

		return date;
		
	}

	public static Date getDateFromString(String input, String format){
		
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		//SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		Date date = null;

		try {

			date = formatter.parse(input);

		} catch (ParseException e) {
			e.printStackTrace();
		}

		return date;
		
	}
	
	public static String getStringFromDate(Date input){
		
		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		String sDate = null;

		sDate = formatter.format(input);

		//System.out.println("getStringFromDate :: sDate : " + sDate);
		return sDate;
		
	}

	public static KeyByDate closestDate(List<KeyByDate> lDates, String fechaAccion) {
		//System.out.println("closestDate ::::::::::::::::::::::::");
		Collections.sort(lDates);
		int index = 0;
		KeyByDate result = null;
		for (Iterator<KeyByDate> iterator = lDates.iterator(); iterator.hasNext();) {
			//System.out.println("getStringFromDate enttra en for ::::::::::::::::::::::::");
			KeyByDate kbd = iterator.next();
			Date date = kbd.getFecLanz();
			if (DateUtils.getTimeFromString(fechaAccion) > date.getTime() || DateUtils.getTimeFromString(fechaAccion) == date.getTime() ){
				//System.out.println("getStringFromDate enttra en if ::::::::::::::::::::::::");
				index = lDates.indexOf(kbd);
				result = kbd;
			}
		}
		//System.out.println("closestDate ini :::::::::::::::::::::::: resultDate es :" + resultDate);
		return result;
				//!= null ? DateUtils.getStringFromDate(resultDate) : null;
	}

	public static void main(String[] args){
		Date sdfsdfss = getDateFromString("08/09/2015 0:58:34");
		String sfsffss = getStringFromDate(sdfsdfss);
		System.out.println("ssfs");
		
	}

	public static KeyByDate closestDate(List<KeyByDate> lDates, String fechaAccion, String format) {
		//System.out.println("closestDate ::::::::::::::::::::::::");
		Collections.sort(lDates);
		int index = 0;
		KeyByDate result = null;
		for (Iterator<KeyByDate> iterator = lDates.iterator(); iterator.hasNext();) {
			//System.out.println("getStringFromDate enttra en for ::::::::::::::::::::::::");
			KeyByDate kbd = iterator.next();
			Date date = kbd.getFecLanz();
			if (DateUtils.getTimeFromString(fechaAccion, format) > date.getTime() || DateUtils.getTimeFromString(fechaAccion, format) == date.getTime() ){
				//System.out.println("getStringFromDate enttra en if ::::::::::::::::::::::::");
				index = lDates.indexOf(kbd);
				result = kbd;
			}
		}
		//System.out.println("closestDate ini :::::::::::::::::::::::: resultDate es :" + resultDate);
		return result;
				//!= null ? DateUtils.getStringFromDate(resultDate) : null;
	}
}
