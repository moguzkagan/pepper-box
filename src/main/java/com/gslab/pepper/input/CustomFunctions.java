package com.gslab.pepper.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The CustomFunctions allows users to write custom functions and then it can be
 * used in template.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate
 *         <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class CustomFunctions {
	private static final Logger log = LogManager.getLogger(CustomFunctions.class.getName());

	public static String RANDOM_LINE_FROM_FILE(String filePath) {
		try (RandomAccessFile file = new RandomAccessFile(new File(filePath), "r")) {
			Random random = new Random();
			int trys = 0;
			String str = "";
			do {
				trys++;
				int pos = random.nextInt((int) file.length());
				file.seek(pos);
				file.readLine();
				str = file.readLine();
			} while (str == null || trys < 5);

			return str;
		} catch (FileNotFoundException e) {
			log.error("File not found ", e);
			return null;
		} catch (IOException e) {
			log.error("Failed get line from file", e);
			return null;
		}
	}

	public static String GET_UNIQUE_DATA_FROM_FILE(String filePath) {
		try (RandomAccessFile file = new RandomAccessFile(new File(filePath), "r")) {
			String str = "";
			file.seek(0);
			str = file.readLine();
			return str;
		} catch (FileNotFoundException e) {
			log.error("File not found ", e);
			return null;
		} catch (IOException e) {
			log.error("Failed get line from file", e);
			return null;
		}
	}

	private static void printUsage() {
		OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
		for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
			method.setAccessible(true);
			if (method.getName().startsWith("get") && Modifier.isPublic(method.getModifiers())) {
				Object value;
				try {
					value = method.invoke(operatingSystemMXBean);
				} catch (Exception e) {
					value = e;
				} // try
				log.debug(method.getName() + " = " + value);
			} // if
		} // for
	}
}
