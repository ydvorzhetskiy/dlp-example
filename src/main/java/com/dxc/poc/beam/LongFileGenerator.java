package com.dxc.poc.beam;

import java.io.FileWriter;
import java.io.Writer;

public class LongFileGenerator {
    public static void main(String[] args) throws Exception {
        try (Writer fw = new FileWriter("./in.long.txt")) {
            for (int i = 0; i < 1000; ++i) {
                fw.write("{" +
                    "\"pr_locator_id\": \"FGDNKU\", \"ticket_number\":" +
                    String.format("\"1c345a781%04d\"", i) +
                    "," +
                    "\"pr_create_date\": \"2021-05-11\", \"pr_sequence\": \"1\"," +
                    "\"from_datetime\": \"2021-05-11 13:35:12\", \"tr_datetime\": \"2021-05-11 13:35:12\"," +
                    "\"creditcard\": [{ \"issuing_network\": \"Visa\"," +
                    "\"card_number\": " +
                    String.format("\"448576421997%04d\"", i) +
                    "}]}\n");
            }
        }
    }
}
