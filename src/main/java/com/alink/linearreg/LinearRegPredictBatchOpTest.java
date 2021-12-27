package com.alink.linearreg;

import org.apache.flink.types.Row;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LinearRegPredictBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;



public class LinearRegPredictBatchOpTest {


    @Test
    public void testLinearRegPredictBatchOp() throws Exception {

        String filePath = "src/main/resources/model/LinearRegPredictModel";

        AkSourceBatchOp model = new AkSourceBatchOp().setFilePath(filePath);
    
        List <Row> df = Arrays.asList(
                Row.of(2, 1),
                Row.of(3, 2),
                Row.of(4, 3),
                Row.of(2, 4),
                Row.of(2, 2),
                Row.of(4, 3),
                Row.of(1, 2),
                Row.of(5, 3)
        );
        BatchOperator <?> batchData = new MemSourceBatchOp(df, "f0 int, f1 int");
        BatchOperator <?> predictor = new LinearRegPredictBatchOp()
                .setPredictionCol("pred");
        predictor.linkFrom(model, batchData).print();
    }
}
