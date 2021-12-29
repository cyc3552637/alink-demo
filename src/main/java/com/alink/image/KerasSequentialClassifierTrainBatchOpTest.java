package com.alink.image;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.KerasSequentialClassifierPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.KerasSequentialClassifierTrainBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelPredictBatchOp;
import org.junit.Test;
public class KerasSequentialClassifierTrainBatchOpTest {
    @Test
    public void testKerasSequentialClassifierTrainBatchOp() throws Exception {
        PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();
        pluginDownloader.localResourcePluginPath("plugins/resources/tf231_python_env_windows","0.02");
        pluginDownloader.localResourcePluginPath("plugins/flink-1.13/tf_predictor_windows","0.02");
//        pluginDownloader.downloadPlugin("tf231_python_env_windows"); // change according to system type
//        pluginDownloader.downloadPlugin("tf_predictor_windows"); // change according to system type
        BatchOperator<?> source = new CsvSourceBatchOp()
                .setFilePath("src/main/resources/image_csv/image.csv")
                .setSchemaStr("path string,label int,tensor string");
//                .setFilePath("src/main/resources/image_csv/random_tensor.csv")
//                .setSchemaStr("tensor string,label int");
        KerasSequentialClassifierTrainBatchOp trainBatchOp = new KerasSequentialClassifierTrainBatchOp()
                .setTensorCol("tensor")
                .setLabelCol("label")
                .setLayers(new String[] {
                        "Conv2D(32, (3, 3), padding='same', activation='relu', input_shape=(150, 150, 3))",
                        "MaxPooling2D(strides=(2, 2))",
                        "Conv2D(64, (3, 3), padding='same', activation='relu')",
                        "MaxPooling2D(strides=(2, 2))",
                        "Conv2D(128, (3, 3), padding='same', activation='relu')",
                        "MaxPooling2D(strides=(2, 2))",
                        "Flatten()"
                })
                .setOptimizer("Adam()")
                .setNumEpochs(1)
                .setBatchSize(1)
                .linkFrom(source);
        KerasSequentialClassifierPredictBatchOp predictBatchOp = new KerasSequentialClassifierPredictBatchOp()
                .setPredictionCol("pred")
                .setPredictionDetailCol("pred_detail")
                .setReservedCols("label")
                .linkFrom(trainBatchOp, source);
        predictBatchOp.lazyPrint(10);
        BatchOperator.execute();
//        TFTableModelPredictBatchOp tfTableModelPredictBatchOp = new TFTableModelPredictBatchOp()
//                .setOutputSchemaStr("logits double")
//                .setOutputSignatureDefs(new String[]{"logits"})
//                .setSignatureDefKey("predict")
//                .setSelectedCols("label")
//                .linkFrom(trainBatchOp, source);
//        tfTableModelPredictBatchOp.print();
    }

}
