package com.dxc.poc.beam.dlp;

import com.dxc.poc.beam.dto.Pnr;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.common.io.BaseEncoding;
import com.google.privacy.dlp.v2.*;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.util.Arrays;

public class CreditCardMasking extends DoFn<String, String> {

    @ProcessElement
    public void processElement(@Element String original, OutputReceiver<String> out) throws Exception {
        try (DlpServiceClient dlp = DlpServiceClient.create()) {
            // Specify what content you want the service to DeIdentify
            ContentItem contentItem = ContentItem.newBuilder().setValue(original).build();

            InfoType infoType = InfoType.newBuilder().setName("CREDIT_CARD_NUMBER").build();
            InspectConfig inspectConfig =
                InspectConfig.newBuilder().addAllInfoTypes(Arrays.asList(infoType)).build();

            // Specify an encrypted AES-256 key and the name of the Cloud KMS key that encrypted it
            CryptoKey cryptoKey = CryptoKey.newBuilder().setUnwrapped(
                UnwrappedCryptoKey.newBuilder().setKey(ByteString.copyFromUtf8("1234567812345678")).buildPartial()
            ).build();

            // Specify how the info from the inspection should be encrypted.
            InfoType surrogateInfoType = InfoType.newBuilder().setName("SSN_TOKEN").build();
            CryptoReplaceFfxFpeConfig cryptoReplaceFfxFpeConfig =
                CryptoReplaceFfxFpeConfig.newBuilder()
                    .setCryptoKey(cryptoKey)
                    .setCommonAlphabet(CryptoReplaceFfxFpeConfig.FfxCommonNativeAlphabet.ALPHA_NUMERIC)
                    .setSurrogateInfoType(surrogateInfoType)
                    .build();
            PrimitiveTransformation primitiveTransformation =
                PrimitiveTransformation.newBuilder()
                    .setCryptoReplaceFfxFpeConfig(cryptoReplaceFfxFpeConfig)
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

            // Print the results
            out.output(original);
        }
    }
}
