package com.dxc.poc.beam.dlp;

import com.dxc.poc.beam.dto.Pnr;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

public class CreditCardMasking extends DoFn<String, String> {

    @ProcessElement
    public void processElement(@Element String original, OutputReceiver<String> out) {
        out.output(original);
    }
}
