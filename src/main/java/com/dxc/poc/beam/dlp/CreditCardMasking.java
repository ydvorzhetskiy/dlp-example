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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class CreditCardMasking extends DoFn<Pnr, Pnr> {

    @ProcessElement
    public void processElement(@Element Pnr original, OutputReceiver<Pnr> out) throws Exception {
        // Specify an encrypted AES-256 key and the name of the Cloud KMS key that encrypted it
        CryptoKey cryptoKey = CryptoKey.newBuilder().setUnwrapped(
            UnwrappedCryptoKey.newBuilder().setKey(ByteString.copyFromUtf8("1234567812345678")).buildPartial()
        ).build();

        try {
            FormatPreservingEncryption formatPreservingEncryption = FormatPreservingEncryptionBuilder
                .ff1Implementation()
                .withDefaultDomain()
                .withDefaultPseudoRandomFunction(new byte[]{
                    (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                    (byte) 0x01, (byte) 0x01, (byte) 0x01, (byte) 0x01,
                    (byte) 0x02, (byte) 0x02, (byte) 0x02, (byte) 0x02,
                    (byte) 0x03, (byte) 0x03, (byte) 0x03, (byte) 0x03
                })
                .withDefaultLengthRange()
                .build();

            String result = formatPreservingEncryption.encrypt(original.getTicketNumber(), new byte[]{
                (byte) 0x01, (byte) 0x03, (byte) 0x02, (byte) 0x04
            });
            original.setTicketNumber(result);
        } catch (Exception ex) {
            // here is nothing to do
        }
        out.output(original);
    }
}
