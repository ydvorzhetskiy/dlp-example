package com.dxc.poc.beam.dlp;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.*;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.Arrays;

public class CreditCardMasking extends DoFn<String, String> {

    @ProcessElement
    public void processElement(@Element String original, OutputReceiver<String> out) throws Exception {
        try (DlpServiceClient dlp = DlpServiceClient.create()) {

            // Specify what content you want the service to DeIdentify
            ContentItem contentItem = ContentItem.newBuilder().setValue(original).build();

            InfoType infoType = InfoType.newBuilder()
                .setName("CREDIT_CARD_NUMBER").build();
            InspectConfig inspectConfig =
                InspectConfig.newBuilder().addAllInfoTypes(Arrays.asList(infoType)).build();

            // Specify how the info from the inspection should be masked.
            CharacterMaskConfig characterMaskConfig =
                CharacterMaskConfig.newBuilder()
                    .setMaskingCharacter("#")
                    .setNumberToMask(12)
                    .build();
            PrimitiveTransformation primitiveTransformation =
                PrimitiveTransformation.newBuilder()
                    .setReplaceWithInfoTypeConfig(ReplaceWithInfoTypeConfig.getDefaultInstance())
                    .build();
            InfoTypeTransformations.InfoTypeTransformation infoTypeTransformation =
                InfoTypeTransformations.InfoTypeTransformation.newBuilder()
                    .setPrimitiveTransformation(primitiveTransformation)
                    .build();
            InfoTypeTransformations transformations =
                InfoTypeTransformations.newBuilder().addTransformations(infoTypeTransformation).build();

            DeidentifyConfig deidentifyConfig =
                DeidentifyConfig.newBuilder().setInfoTypeTransformations(transformations).build();

            // Combine configurations into a request for the service.
            DeidentifyContentRequest request =
                DeidentifyContentRequest.newBuilder()
                    .setParent(LocationName.of("sabre-cdw-dev-sandbox", "us-central1").toString())
                    .setItem(contentItem)
                    .setInspectConfig(inspectConfig)
                    .setDeidentifyConfig(deidentifyConfig)
                    .build();

            // Send the request and receive response from the service
            DeidentifyContentResponse response = dlp.deidentifyContent(request);

            out.output(response.getItem().getValue());
        }

    }
}
