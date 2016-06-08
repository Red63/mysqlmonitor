package com.retail.datahub.es.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


public class DateUtil {

	/** 日期+时间的格式 */
	final static public String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	/** 日期的格式 */
	final static public String DATE_FORMAT = "yyyy-MM-dd";
	final static public String MONTH_FORMAT = "yyyy-MM";
	/** 中文日期的格式 */
	final static public String DATE_CH_FORMAT = "yyyy年MM月dd日";
	/** 小时:分的格式 */
	final static public String HHMM_FORMAT = "HH:mm";

	final static public String HHMMSS_FORMAT = "HH:mm:ss";

	final static public String DATE_TIME_SSS_FORMAT = "yyyy-MM-dd HH:mm:ss SSS";

	/**
	 * 将日期字符串解析成指定格式的Date对象
	 * 
	 * @param dateTime
	 *            日期字符串
	 * @param format
	 *            指定格式
	 * @return （正确格式）日期对象
	 * @throws java.text.ParseException
	 */
	public static Date parse(String dateTime, String format)
			throws ParseException {
		if (dateTime == null || dateTime.length() <= 0)
			return null;
		String sDateTime = ((dateTime.indexOf('.') > 0)) ? dateTime.substring(
				0, dateTime.indexOf('.')) : dateTime;

		DateFormat dateFormat = new SimpleDateFormat(format);
		return dateFormat.parse(sDateTime);
	}

	/**
	 * 将日期字符串解析成"yyyy-MM-dd HH:mm:ss"格式的Date对象
	 *
	 * @param dateTime
	 *            日期字符串
	 *            指定格式
	 * @return （正确格式）日期对象
	 * @throws java.text.ParseException
	 */
	public static Date parseDateTime(String dateTime) throws ParseException {
		return parse(dateTime, DATE_TIME_FORMAT);
	}

	/**
	 * 将日期类解析成指定格式的日期字符串
	 *
	 * @param date
	 * @param format
	 * @return
	 */
	public static String formatDateTime(Date date, String format) {
		if (date == null)
			return "";
		DateFormat dateFormat = new SimpleDateFormat(format);
		return dateFormat.format(date);
	}

	/**
	 * 将日期字符串解析成"yyyy-MM-dd"格式的Date对象
	 *
	 * @param dateTime
	 *            日期字符串
	 *            指定格式
	 * @return （正确格式）日期对象
	 * @throws java.text.ParseException
	 */
	public static Date parseDate(String dateTime) throws ParseException {
		return parse(dateTime, DATE_FORMAT);
	}

	/**
	 * 将日期字符串解析成"yyyy年MM月dd日"格式的Date对象
	 *
	 * @param dateTime
	 *            日期字符串
	 *            指定格式
	 * @return （正确格式）日期对象
	 * @throws java.text.ParseException
	 */
	public static Date parseChDate(String dateTime) throws ParseException {
		return parse(dateTime, DATE_CH_FORMAT);
	}

	/**
	 * 将日期类解析成"HH:mm"格式的日期字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String formatHHmm(Date date) {
		return formatDateTime(date, HHMM_FORMAT);
	}

	/**
	 * 将日期类解析成"HH:mm:ss"格式的日期字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String formatHHmmss(Date date) {
		return formatDateTime(date, HHMMSS_FORMAT);
	}

	/**
	 * 将日期类解析成"yyyy-MM-dd"格式的日期字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String formatYYYYMMDD(Date date) {
		return formatDateTime(date, DATE_FORMAT);
	}
	
	/**
	 * 将日期类解析成"yyyy-MM"格式的日期字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String formatYYYYMM(Date date) {
		return formatDateTime(date, MONTH_FORMAT);
	}

	/**
	 * 将日期类解析成"yyyy年MM月dd日"格式的日期字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String formatChYYYYMMDD(Date date) {
		return formatDateTime(date, DATE_CH_FORMAT);
	}

	/**
	 * 将日期类解析成"yyyy-MM-dd HH:mm:ss"格式的日期字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String format(Date date) {
		return formatDateTime(date, DATE_TIME_FORMAT);
	}

	/**
	 * 
	 * 获取当前时间
	 * 
	 */
	public static String getCurrDate() {
		return format(new Date());
	}

	/**
	 * 
	 * 转换日期字符串
	 * 
	 * @throws Exception
	 * 
	 */
	public static Date translateDate(String date) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.parse(date);
	}

	/**
	 * 返回某一天的零点的日期字符串
	 * 
	 * @param date
	 * @return
	 */
	public static Date toZeroDateTime(Date date) {
		String dateStr = formatDateTime(date, DATE_FORMAT) + " 00:00:00";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			date = sdf.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
	
	/**
	 * 返回某一天的零点的日期字符串
	 * 
	 * @param date
	 * @return
	 */
	public static Date toYyyymmddDateTime(Date date) {
		String dateStr = formatDateTime(date, DATE_FORMAT);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			date = sdf.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}

	/**
	 * 某年某月某天的上一天 如果输入1900年1月1日，则返回1900年1月1日。
	 * 
	 * @param date
	 * 
	 * @return
	 */
	public static Date getPrevDay(Date date) {
		if (date == null)
			return null;
		int year = getYear(date);
		int month = getMonth(date);
		int day = getMonthDay(date);
		if (year < 1900 || month < 0 || month > 11 || day < 1 || day > 31)
			return null;

		date = getPrevDay(year, month, day);
		try {
			date = parseDateTime(toZeroDate(date));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}

	/**
	 * 某年某月某天的上一天 如果输入1900年1月1日，则返回1900年1月1日。
	 * 
	 * @param year
	 * @param month
	 * @param day
	 * 
	 * @throws RuntimeException
	 * @return
	 */
	public static Date getPrevDay(int year, int month, int day) {
		if (!checkDay(year, month, day))
			return null;

		// 如果当前日期为1900-1-1 前一天仍为当天
		if (year == 1900 && month == 0 && day == 1) {
			return toDate(1900, 0, 1);
		}

		Calendar cal = GregorianCalendar.getInstance();
		cal.set(year, month, day);
		cal.add(Calendar.DAY_OF_YEAR, -1);

		return cal.getTime();
	}

	/**
	 * 某年某月某天的下一天,并格式化成YYYY-MM-DD
	 * 
	 * @param date
	 * @return
	 */
	public static Date getNextDay(Date date) {
		if (date == null)
			return null;
		int year = getYear(date);
		int month = getMonth(date);
		int day = getMonthDay(date);
		date = getNextDay(year, month, day);
		try {
			date = parseDateTime(toZeroDate(date));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}

	/**
	 * 返回某一天的零点的日期字符串
	 * 
	 * @param date
	 * @return
	 */
	public static String toZeroDate(Date date) {
		return formatDateTime(date, DATE_FORMAT) + " 00:00:00";
	}

	/**
	 * 某年某月某天的下一天
	 * 
	 * @param year
	 * @param month
	 * @param monthDay
	 * 
	 * @return
	 */
	public static Date getNextDay(int year, int month, int day) {
		if (!checkDay(year, month, day))
			return null;

		Calendar cal = GregorianCalendar.getInstance();
		cal.set(year, month, day);
		cal.add(Calendar.DAY_OF_YEAR, 1);

		return cal.getTime();
	}

	/**
	 * 返回某一天最后时刻的日期
	 * 
	 * @param date
	 * @return
	 */
	public static Date toLastDateTime(Date date) {
		if (date == null)
			return null;
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.HOUR_OF_DAY, 23);
		calendar.set(Calendar.MINUTE, 59);
		calendar.set(Calendar.SECOND, 59);
		return calendar.getTime();
	}
	
	/**
	 * 返回某一天最后时刻的日期
	 * 
	 * @param date
	 * @return
	 */
	public static Date toFirstDateTime(Date date) {
		if (date == null)
			return null;
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return calendar.getTime();
	}

	/**
	 * 比较日期：年月日。
	 * 
	 * @param d0
	 * @param d1
	 * @return the value <code>0</code> if the argument d1 is equal to d0; a
	 *         value less than <code>0</code> if d0 is before d1 argument; and a
	 *         value greater than <code>0</code> if d0 is after d1 argument.
	 * @exception NullPointerException
	 *                if d0 or d1 is null.
	 */
	public static int compareDay(Date d0, Date d1) {
		return d0.compareTo(d1);
	}

	/**
	 * d0是否在[d1,d2]的日期区间中
	 * 
	 * @param d0
	 * @param d1
	 * @param d2
	 * @return
	 * @exception NullPointerException
	 *                if d0 or d1 or d2 is null.
	 */
	public static boolean isInDayZone(Date d0, Date d1, Date d2) {
		return (compareDay(d0, d1) >= 0 && compareDay(d1, d2) <= 0) ? true
				: false;
	}

	/**
	 * 返回N天之后（之前）的某天。 例外情况：如找到的那天晚于1900-1-1，则返回1900-1-1。
	 * 
	 * @param date
	 * @param days
	 * @return
	 */
	public static Date getThisDayByDay(Date date, int days) {
		if (days == 0)
			return date;
		if (date == null)
			return null;

		int year = getYear(date);
		int month = getMonth(date);
		int day = getMonthDay(date);
		if (!checkDay(year, month, day))
			return null;
		Calendar cal = GregorianCalendar.getInstance();
		cal.set(year, month, day);
		cal.add(Calendar.DAY_OF_YEAR, days);

		Date findDate = cal.getTime();
		Date date_1900_1_1 = toDate(1900, 0, 1);
		if (compareDay(findDate, date_1900_1_1) < 0)
			return date_1900_1_1;

		return findDate;
	}

	/**
	 * 某年某月的天数。
	 * 
	 * @param year
	 * @param month
	 * @return
	 */
	public static int getMaxDays(int year, int month) {
		if (year < 1900 || month < 0 || month > 11)
			return 0;

		Calendar cal = GregorianCalendar.getInstance();
		cal.set(year, month, 1);

		return cal.getActualMaximum(Calendar.DAY_OF_MONTH);
	}

	public static boolean checkDay(int year, int month, int monthDay) {
		if (year < 1900 || month < 0 || month > 11 || monthDay < 1
				|| monthDay > 31)
			return false;
		int maxDay = getMaxDays(year, month);
		if (monthDay > maxDay)
			return false;
		return true;
	}

	/**
	 * 返回日期
	 * 
	 * @param year
	 *            1900-...
	 * @param month
	 *            0-11
	 * @param day
	 *            1-31
	 * 
	 * @throws RuntimeException
	 * @return
	 */
	public static Date toDate(int year, int month, int day) {
		if (!checkDay(year, month, day))
			return null;

		month -= 1;

		if (month <= 0) month = 0;
		if (month >= 11) month = 11;

		Calendar calendar = new GregorianCalendar(year, month, day);

		return calendar.getTime();
	}

	/**
	 * 返回四位年份
	 * 
	 * @param date
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static int getYear(Date date) {
		if (date == null)
			return -1;
		return date.getYear() + 1900;
	}

	/**
	 * 返回月数(0-11)
	 * 
	 * @param date
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static int getMonth(Date date) {
		if (date == null)
			return -1;
		return date.getMonth();
	}

	/**
	 * 好购首页特定日期显示格式，返回格式如(1月30日--2月3日)
	 * 
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static String getMonthAndDay(Date startDate, Date endDate) {
		if (startDate == null || endDate == null)
			return "";
		return startDate.getMonth() + 1 + "月" + startDate.getDate() + "日--"
				+ (endDate.getMonth() + 1) + "月" + endDate.getDate() + "日";
	}

	/**
	 * 返回月几(1-31)
	 * 
	 * @param date
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static int getMonthDay(Date date) {
		if (date == null)
			return -1;
		return date.getDate();
	}

	/**
	 * 返回周几 returned value (<tt>7</tt> = Sunday, <tt>1</tt> = Monday, <tt>2</tt>
	 * = Tuesday, <tt>3</tt> = Wednesday, <tt>4</tt> = Thursday, <tt>5</tt> =
	 * Friday, <tt>6</tt> = Saturday)
	 * 
	 * @param date
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static int getWeekDay(Date date) {
		if (date == null)
			return -1;
		int d = date.getDay();
		if (d == 0)
			d = 7;

		return d;
	}

	/**
	 * 返回小时数(0-23)
	 * 
	 * @param date
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static int getHours(Date date) {
		if (date == null)
			return -1;
		return date.getHours();
	}

	/**
	 * 返回分钟数（0-59）
	 * 
	 * @param date
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static int getMinutes(Date date) {
		if (date == null)
			return -1;
		return date.getMinutes();
	}

	/**
	 * 某年某月某日的所在周的第一天(周一)。
	 * 
	 * @param date
	 * 
	 * @return
	 */
	public static Date getFirstDayOfWeek(Date date) {
		if (date == null)
			return null;

		int year = getYear(date);
		int month = getMonth(date);
		int day = getMonthDay(date);

		return getFirstDayOfWeek(year, month, day);
	}

	/**
	 * 某年某月某日的所在周的第一天(周一)。
	 * 
	 * @param year
	 * @param month
	 * @param day
	 * 
	 * @return
	 */
	public static Date getFirstDayOfWeek(int year, int month, int day) {
		if (!checkDay(year, month, day))
			return null;

		Calendar cal = GregorianCalendar.getInstance();
		cal.set(year, month, day);
		cal.add(Calendar.DAY_OF_WEEK, 1 - getWeekDay(toDate(year, month, day)));

		return cal.getTime();
	}
	
	/**
	 * 某年某月某日的所在周的周末一天(周日)。
	 * 
	 * @param date
	 * @return
	 */
	public static Date getEndDayOfWeek(Date date) {
		if (date == null)
			return null;

		int year = getYear(date);
		int month = getMonth(date);
		int day = getMonthDay(date);

		return getEndDayOfWeek(year, month, day);
	}

	/**
	 * 某年某月某日的所在周的周末一天(周日)。
	 * 
	 * @param year
	 * @param month
	 * @param day
	 * 
	 * @return
	 */
	public static Date getEndDayOfWeek(int year, int month, int day) {
		if (!checkDay(year, month, day))
			return null;

		Calendar cal = GregorianCalendar.getInstance();
		cal.set(year, month, day);
		cal.add(Calendar.DAY_OF_WEEK, 7 - getWeekDay(toDate(year, month, day)));

		return cal.getTime();
	}
	
	/**
	 * 某年某月某日的所在周的前一周第一天(周一)。 如果输入1900年1月1日，则返回1900年1月1日。
	 * 
	 * @param date
	 * 
	 * @return
	 */
	public static Date getFirstDayOfPrevWeek(Date date) {
		if (date == null)
			return null;

		int year = getYear(date);
		int month = getMonth(date);
		int day = getMonthDay(date);

		return getFirstDayOfPrevWeek(year, month, day);
	}
	
	/**
	 * 某年某月某日的所在周的前一周第一天(周一)。 如果输入1900年1月1日，则返回1900年1月1日。
	 * 
	 * @param year
	 * @param month
	 * @param day
	 * 
	 * @return
	 */
	public static Date getFirstDayOfPrevWeek(int year, int month, int day) {
		if (!checkDay(year, month, day))
			return null;

		// 如果当前日期为1900-1-1至1900-1-7中任意一天 上一周的第一天
		if (year == 1900 && month == 0 && day <= 7) {
			return toDate(year, month, 1);
		}

		Calendar cal = GregorianCalendar.getInstance();
		cal.set(year, month, day);
		cal.add(Calendar.DAY_OF_YEAR, -7);
		Date newDate = cal.getTime();

		return getFirstDayOfWeek(getYear(newDate), getMonth(newDate), getMonthDay(newDate));
	}
	
	/**
	 * 得到开始时间的下1秒时间,开始时间为null时得到当前时间的下1秒时间
	 * 
	 * @param startDate
	 * @return
	 */
	public static Date nextSecond(Date startDate) {
		Calendar cal = Calendar.getInstance();
		if (startDate != null) {
			cal.setTime(startDate);
		}
		cal.add(Calendar.SECOND, 1);
		return cal.getTime();
	}
	
	/**
	 * 得到时间的下N分时间,时间为null时得到当前时间的下N分时间
	 * 
	 * @param date
	 * @param minute
	 * @return
	 */
	public static Date nextMinutes(Date date,int minute) {
		Calendar cal = Calendar.getInstance();
		if (date != null) {
			cal.setTime(date);
		}
		cal.add(Calendar.MINUTE, minute);
		return cal.getTime();
	}
	
	/**
	 * 某年某月的第一天
	 * 
	 * @param date
	 * @return
	 */
	public static Date getFirstDayOfMonth(Date date) {
		int year = getYear(date);
		int month = getMonth(date);
		return toDate(year, month, 1);
	}

	/*public static void main(String[] args) {
		System.out.println(toDate(1986, 2, 6));
	}*/
}
