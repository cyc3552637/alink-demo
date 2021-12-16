package com.alink.linearreg;

import org.apache.flink.types.Row;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LinearRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
public class LinearRegTrainBatchOpTest {
    @Test
    public void testLinearRegTrainBatchOp() throws Exception {
        List <Row> df = Arrays.asList(
                Row.of(2, 1, 1),
                Row.of(3, 2, 1),
                Row.of(4, 3, 2),
                Row.of(2, 4, 1),
                Row.of(2, 2, 1),
                Row.of(4, 3, 2),
                Row.of(1, 2, 1),
                Row.of(5, 3, 3)
        );
        BatchOperator <?> batchData = new MemSourceBatchOp(df, "f0 int, f1 int, label int");
        BatchOperator <?> lr = new LinearRegTrainBatchOp()
                .setFeatureCols("f0", "f1")
                .setLabelCol("label");
        BatchOperator model = batchData.link(lr);
        BatchOperator <?> predictor = new LinearRegPredictBatchOp()
                .setPredictionCol("pred");
        predictor.linkFrom(model, batchData).print();

        //5、保存训练结果
        CsvSinkBatchOp csvSink = new CsvSinkBatchOp();
        csvSink.setFilePath("E:\\Flink\\FlinkSql\\FlinkML\\model\\testGbdtRegPredictStreamOp");
        predictor.link(csvSink);

        //6、保存模型
        AkSinkBatchOp csvSink1 = new AkSinkBatchOp();
        csvSink1.setFilePath("E:\\Flink\\FlinkSql\\FlinkML\\model\\testGbdtRegPredictStreamOpModel");
        model.link(csvSink1);

        BatchOperator.execute();
    }
}
