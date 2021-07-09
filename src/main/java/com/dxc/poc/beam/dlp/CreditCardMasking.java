package com.dxc.poc.beam.dlp;

import com.dxc.poc.beam.dto.Pnr;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
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
        byte[] cryptoKey = null;
        try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
            cryptoKey = client.getCryptoKey("projects/sabre-cdw-dev-sandbox/locations/global/keyRings/dlp-keyring/cryptoKeys/dlp-key").toByteArray();
            System.out.println("Key: " + Arrays.toString(cryptoKey));
            System.out.println("Key: " + new String(cryptoKey));
        } catch (IOException e) {
            System.out.println("Key error: " + e.getMessage());
            throw new RuntimeException(e);
        }

        try {
            FormatPreservingEncryption formatPreservingEncryption = FormatPreservingEncryptionBuilder
                .ff1Implementation()
                .withDefaultDomain()
                .withDefaultPseudoRandomFunction(cryptoKey)
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
