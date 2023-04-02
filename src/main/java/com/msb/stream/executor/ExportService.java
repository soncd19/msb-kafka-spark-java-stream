package com.msb.stream.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExportService implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ExportService.class);
    private ExecutorService executorService = null;

    public void process(String pathName, String fileName, String data) {
        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(30);
            logger.info("Created executor service before export data");
        }
        executorService.submit(new ExportExcelExecutor(pathName, fileName, data));
        logger.info("Submit job executor success");
    }
}
