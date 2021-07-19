package com.dxc.poc.beam.dlp;

import com.idealista.fpe.config.Alphabet;
import lombok.val;
import org.apache.commons.lang3.ArrayUtils;

public class AlphaNumericAlphabet implements Alphabet {

    private final static char[] ALPHABET;

    static {
        val eng = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
        val numbers = "0123456789".toCharArray();
        ALPHABET = ArrayUtils.addAll(eng, numbers);
    }

    @Override
    public char[] availableCharacters() {
        return ALPHABET;
    }

    @Override
    public Integer radix() {
        return ALPHABET.length;
    }
}
