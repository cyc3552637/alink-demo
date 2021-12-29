package com.alink.image;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.image.ReadImageToTensorBatchOp;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import org.apache.flink.types.Row;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.io.File;
import java.util.*;

public class ReadImageToTensorBatchOpTest {
    @Test
    public void testReadImageToTensorBatchOp() throws Exception {

        List <Row> data=this.getFiles("src/main/resources/animal_photos");
        MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "path string,label int");
        ReadImageToTensorBatchOp readImageToTensorBatchOp=new ReadImageToTensorBatchOp()
                .setRootFilePath("src/main/resources/animal_photos")
                .setOutputCol("tensor")
                .setRelativeFilePathCol("path")
                .setImageHeight(150)
                .setImageWidth(150)
                .linkFrom(memSourceBatchOp)
                .print();
        CsvSinkBatchOp csvSink = new CsvSinkBatchOp();
        csvSink.setFilePath("src/main/resources/image_csv/image.csv");
        readImageToTensorBatchOp.link(csvSink);
        BatchOperator.execute();
    }
    public static List<Row> getFiles(String filePath) {
        List<Row> filelist=new ArrayList();
        File root = new File(filePath);
        if (!root.exists()) {
            System.out.println("文件夹是空的!");
        } else {
            File[] files = root.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    getFiles(file.getAbsolutePath());
                } else {
                    filelist.add(Row.of(file.getName(),Integer.parseInt(file.getName().substring(0,1))));
                }
            }
        }
        return filelist;
    }

}
