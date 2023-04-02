package com.msb.stream.executor;

import com.msb.stream.utils.WriteExcel;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class ExportExcelExecutor implements Runnable, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ExportExcelExecutor.class);
    private final String pathName;
    private final String fileName;
    private final String data;

    public ExportExcelExecutor(String pathName, String fileName, String data) {
        this.pathName = pathName;
        this.fileName = fileName;
        this.data = data;
    }

    @Override
    public void run() {
        if (data != null && !data.isEmpty()) {
            try {
                WriteExcel.writeExcel2Path(pathName, fileName, new JSONArray(data));
            } catch (Exception e) {
                logger.warn("Pls check your query or your database with: " + e.getMessage(), e);
            }
        }
    }
}
