package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	private String fileName;
	BufferedReader reader = null;

	// parameterized constructor to initialize filename. As you are trying to
	// perform file reading, hence you need to be ready to handle the IO Exceptions.
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		super();
		this.fileName = fileName;
		reader = new BufferedReader(new FileReader(fileName));
	}

	// implementation of getHeader() method. We will have to extract the headers
	// from the first line of the file.
	@Override
	public Header getHeader() throws IOException {

		String header = null;
		// read the first line
		reader = new BufferedReader(new FileReader(fileName));
		header = reader.readLine();
		// populate the header object with the String array containing the header names
		Header headers = new Header(header.split("\\s*,\\s*"));
		return headers;

	}

	/**
	 * This method will be used in the upcoming assignments
	 */
	@Override
	public void getDataRow() {

	}

	 /* implementation of getColumnType() method. To find out the data types, we will
	  * read the first line from the file and extract the field values from it. If a
	  * specific field value can be converted to Integer, the data type of that field
	  * will contain "java.lang.Integer", otherwise if it can be converted to Double,
	  * then the data type of that field will contain "java.lang.Double", otherwise,
	  * the field is to be treated as String.*/
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {

		String row;
		String[] rows;
		String[] dataTypeArray;

		row = reader.readLine();
		rows = row.split("\\s*,\\s*", -1);
		dataTypeArray = new String[rows.length];

		for (int index = 0; index < rows.length; index++) {

			dataTypeArray[index] = findColumnDataType(rows[index]).getClass().getName();
		}

		DataTypeDefinitions dataTypes = new DataTypeDefinitions(dataTypeArray);

		return dataTypes;
	}

	private Object findColumnDataType(String value) {

		int intValue;
		double doubleValue;

		if (value.isEmpty()) {
			return "";
		}

		try {
			intValue = Integer.parseInt(value);
			return intValue;
		} catch (NumberFormatException outerException) {
			try {
				doubleValue = Double.parseDouble(value);
				return doubleValue;
			} catch (NumberFormatException innerException) {

			}

		}

		return value;

	}

}
