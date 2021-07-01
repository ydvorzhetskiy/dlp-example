package com.dxc.poc.beam.dlp;

import com.dxc.poc.beam.dto.Pnr;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.common.io.BaseEncoding;
import com.google.privacy.dlp.v2.*;
import com.google.protobuf.ByteString;
import com.idealista.fpe.FormatPreservingEncryption;
import com.idealista.fpe.builder.FormatPreservingEncryptionBuilder;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.util.Arrays;

public class CreditCardMasking extends DoFn<String, String> {

    @ProcessElement
    public void processElement(@Element String original, OutputReceiver<String> out) throws Exception {
        // Specify an encrypted AES-256 key and the name of the Cloud KMS key that encrypted it
        CryptoKey cryptoKey = CryptoKey.newBuilder().setUnwrapped(
            UnwrappedCryptoKey.newBuilder().setKey(ByteString.copyFromUtf8("1234567812345678")).buildPartial()
        ).build();

        FormatPreservingEncryption formatPreservingEncryption = FormatPreservingEncryptionBuilder
            .ff1Implementation()
            .withDefaultDomain()
            .withDefaultPseudoRandomFunction(cryptoKey.toByteArray())
            .withDefaultLengthRange()
            .build();

        String result = formatPreservingEncryption.encrypt(original, cryptoKey.toByteArray());

        out.output(result);
    }
}
