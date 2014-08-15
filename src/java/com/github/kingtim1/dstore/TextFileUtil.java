package com.github.kingtim1.dstore;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TextFileUtil
{
	public static final String NEWLINE = System.getProperty("line.separator");
	
	/**
	 * Inserts a string into a file at a specified line number. The line number
	 * starts at 0 and the last line number is N-1 where N is the total number
	 * of lines. If the line number is greater than N the string is inserted at
	 * the end of the file.
	 * 
	 * @param insertLine
	 *            the string to be inserted
	 * @param lineNum
	 *            the line number to insert the string at
	 * @param filename
	 *            the name of a text file
	 * @throws IOException
	 *             if an I/O error occurs while either reading or writing
	 *             from/to the file
	 */
	public static final void insertLineIntoFile(String insertLine, int lineNum,
			String filename) throws IOException
	{
		// Read in the entire file
		// Each line is stored in in the list and the line to be inserted is
		// stored in the list at the specified line number
		List<String> lines = new ArrayList<String>(1000);
		Scanner fsc = new Scanner(new File(filename));
		int currentLine = 0;
		while (fsc.hasNextLine()) {
			if (currentLine == lineNum) {
				lines.add(insertLine);
			}
			String line = fsc.nextLine();
			lines.add(line);
			currentLine++;
		}
		if (currentLine <= lineNum) {
			lines.add(insertLine);
		}
		fsc.close();

		// Write the list of strings to the file.
		FileWriter fw = new FileWriter(filename);
		for (int i = 0; i < lines.size(); i++) {
			fw.write(lines.get(i) + NEWLINE);
		}
		fw.close();
	}

	public static final void findReplace(String regex, String replace,
			String filename) throws IOException
	{
		List<String> lines = new ArrayList<String>(1000);
		Scanner fsc = new Scanner(new File(filename));
		while (fsc.hasNextLine()) {
			lines.add(fsc.nextLine());
		}
		fsc.close();
		
		FileWriter fw = new FileWriter(filename);
		for(int i=0;i<lines.size();i++){
			String line = lines.get(i);
			line = line.replaceAll(regex, replace);
			fw.write(line + NEWLINE);
		}
		fw.close();
	}
	
	public static final void replaceLine(String content, String replace, String filename) throws IOException
	{
		List<String> lines = new ArrayList<String>(1000);
		Scanner fsc = new Scanner(new File(filename));
		while (fsc.hasNextLine()) {
			lines.add(fsc.nextLine());
		}
		fsc.close();
		
		FileWriter fw = new FileWriter(filename);
		for(int i=0;i<lines.size();i++){
			String line = lines.get(i);
			if(line.equals(content)){
				line = replace;
			}
			fw.write(line + NEWLINE);
		}
		fw.close();
	}
	
	/**
	 * Deletes all lines from a text file that match a regular expression.
	 * @param txtFile a text file to delete lines from
	 * @param tmpTxtFile a temporary text file
	 * @param regexPattern the regular expression that will determine lines to be deleted
	 * @return the number of deleted lines
	 * @throws IOExcepiton if an I/O error occurs while deleting lines from the text file
	 */
	public static final int deleteLines(File txtFile, File tmpTxtFile, String regexPattern) throws IOException
	{
		BufferedReader reader = new BufferedReader(new FileReader(txtFile));
		BufferedWriter writer = new BufferedWriter(new FileWriter(tmpTxtFile));
		int deleteCount = 0;
		String line = null;
		while((line = reader.readLine()) != null){
			if(!line.matches(regexPattern)){
				writer.write(line + NEWLINE);
			}else{
				deleteCount++;
			}
		}
		
		reader.close();
		writer.close();
		
		txtFile.delete();
		tmpTxtFile.renameTo(txtFile);
		return deleteCount;
	}
}
