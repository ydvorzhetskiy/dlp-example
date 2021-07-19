package com.dxc.poc.beam.dlp;

import com.dxc.poc.beam.dto.Pnr;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.idealista.fpe.FormatPreservingEncryption;
import com.idealista.fpe.builder.FormatPreservingEncryptionBuilder;
import com.idealista.fpe.component.functions.prf.DefaultPseudoRandomFunction;
import com.idealista.fpe.config.GenericDomain;
import com.idealista.fpe.config.GenericTransformations;
import com.idealista.fpe.config.LengthRange;
import lombok.SneakyThrows;
import org.apache.beam.sdk.transforms.DoFn;

public class CreditCardMasking extends DoFn<Pnr, Pnr> {

    private byte[] key = null;
    private AlphaNumericAlphabet alphabet;
    private GenericTransformations genericTransformations;

    @SneakyThrows
    private synchronized void loadKey() {
        if (key == null) {
            try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
                AccessSecretVersionResponse response = client.accessSecretVersion(
                    "projects/652835946224/secrets/dxcSandbox_SecretForFpe/versions/latest");
                key = response.getPayload().getData().toByteArray();
            }
            alphabet = new AlphaNumericAlphabet();
            genericTransformations = new GenericTransformations(alphabet.availableCharacters());
        }
    }

    @ProcessElement
    public void processElement(@Element Pnr original, OutputReceiver<Pnr> out) throws Exception {
        loadKey();
        FormatPreservingEncryption formatPreservingEncryption = FormatPreservingEncryptionBuilder
            .ff1Implementation()
            .withDomain(new GenericDomain(alphabet,
                genericTransformations,
                genericTransformations
            ))
            .withPseudoRandomFunction(new DefaultPseudoRandomFunction(key))
            .withLengthRange(new LengthRange(2, 20))
            .build();

        String result = formatPreservingEncryption.encrypt("123123", "0".getBytes());
        original.setTicketNumber(result);
        out.output(original);
    }
}
