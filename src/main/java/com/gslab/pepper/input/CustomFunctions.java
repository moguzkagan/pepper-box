package com.gslab.pepper.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

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
	private static final Logger log = Logger.getLogger(CustomFunctions.class.getName());

	public static String RANDOM_LINE_FROM_FILE(String filePath) {
		try (RandomAccessFile file = new RandomAccessFile(new File(filePath), "r")) {
			Random random = new Random();
			String str = "";
			do {
				int pos = random.nextInt((int) file.length());
				file.seek(pos);
				file.readLine();
				str = file.readLine();
			} while (str == null);

			return str;
		} catch (FileNotFoundException e) {
			log.log(Level.SEVERE, "File not found ", e);
			return null;
		} catch (IOException e) {
			log.log(Level.SEVERE, "Failed get line from file", e);
			return null;
		}
	}
}
