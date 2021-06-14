package com.dxc.poc.beam;

import com.dxc.poc.beam.pipeline.GsToBqOptions;
import com.dxc.poc.beam.pipeline.PnrGsToBqPipeline;
import lombok.val;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;


public class GsToBqApplication {
    public static void main(String[] args) {
        val options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(GsToBqOptions.class);
        options.setRunner(DataflowRunner.class);

        if (options.getTemplateLocation() != null) {
            try {
                PipelineResult result = Pipeline.create(options).run();
                result.getState();
                result.waitUntilFinish();
            } catch (UnsupportedOperationException e) {
                // do nothing
                // This is workaround for https://issues.apache.org/jira/browse/BEAM-9337
            }
        } else {
            PnrGsToBqPipeline.createAndRunPipeline(options);
        }
    }
}
