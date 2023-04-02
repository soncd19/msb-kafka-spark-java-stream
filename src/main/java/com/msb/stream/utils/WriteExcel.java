package com.msb.stream.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.dhatim.fastexcel.Workbook;
import org.dhatim.fastexcel.Worksheet;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;

// Done import libs

public class WriteExcel {
	static Properties pro = new Properties();
	static int i = 0;
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(WriteExcel.class);
	public static void writeExcel2Path(String pathName,String fileName,JSONArray jsonArray) throws IOException, JSONException {
		String EXPORT_PATH = pathName+"/"+fileName + "_" + UUID.randomUUID() + ".xlsx";
		// For all
		for (int a = 0; a < jsonArray.length(); a++) 
		{
			List<String> lstData = new ArrayList<>();
			List<String> lstFieldName = new ArrayList<>();
			JSONObject jo = (JSONObject) jsonArray.get(a);
			JSONArray ja_Data = (JSONArray) jo.get("Data");
			JSONArray ja_FieldList = (JSONArray) jo.get("FieldList");
			// process ja_FieldList
			for(int i=0; i < ja_FieldList.length(); i++)   
				{  
				JSONObject object = ja_FieldList.getJSONObject(i);  
					lstFieldName.add(object.getString("FieldName"));
				}
			// Create File
			File file = new File(EXPORT_PATH);
			if (file.getPath().isEmpty()) {
				logger.error("Path not Exist");
			} else {
				try (FileOutputStream fo = new FileOutputStream(file)) {
					// Create Workbook
					Workbook wb = new Workbook(fo, "Application", "1.0");
					// Create sheet 1
					Worksheet ws1 = wb.newWorksheet("Sheet1");
					for (int j = 0; j < lstFieldName.size(); j++) {
						if(lstFieldName.get(j) == null || lstFieldName.get(j) == "") {
							ws1.value(0, j, "null");
						}else {
							ws1.value(0, j, lstFieldName.get(j));
						}
					}				
					for (int i = 0; i < ja_Data.length(); i++) {
						JSONObject obj = ja_Data.getJSONObject(i);
						for (int j = 0; j < obj.length(); j++) {
							if(obj.getString(Integer.toString(j)) != null && obj.getString(Integer.toString(j)) != "") {
								ws1.value(i + 1, j, obj.getString(Integer.toString(j)));
							}else
							{
								ws1.value(i + 1, j, "null");
							}
							logger.debug(obj.getString(Integer.toString(j)));
						}
					}
					wb.finish();	
				}
			}
		}
	}
}
