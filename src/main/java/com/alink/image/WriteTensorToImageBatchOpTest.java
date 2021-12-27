package com.alink.image;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.TensorToVectorBatchOp;
import com.alibaba.alink.operator.batch.dataproc.format.CsvToColumnsBatchOp;
import com.alibaba.alink.operator.batch.image.ReadImageToTensorBatchOp;
import com.alibaba.alink.operator.batch.image.WriteTensorToImageBatchOp;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import org.apache.flink.types.Row;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.image.HasImageType.ImageType;
import org.junit.Test;
import java.util.Collections;
import java.util.List;
public class WriteTensorToImageBatchOpTest {
    @Test
    public void testWriteTensorToImageBatchOp() throws Exception {
        CsvSourceBatchOp csvSource = new CsvSourceBatchOp()
                .setFilePath("src/main/resources/image_csv/image.csv")
                .setSchemaStr("path string,label int,tensor string")
                .setFieldDelimiter(",");
        MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(csvSource.collect(), "path string,label int,tensor string");
        WriteTensorToImageBatchOp writeTensorToImageBatchOp = new WriteTensorToImageBatchOp()
                .setRootFilePath("src/main/resources/write_tensor_to_image")
                .setTensorCol("tensor")
                .setImageType(ImageType.PNG)
                .setRelativeFilePathCol("path");

        memSourceBatchOp.link(writeTensorToImageBatchOp).print();
    }
}