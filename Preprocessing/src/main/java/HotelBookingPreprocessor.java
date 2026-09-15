import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class HotelBookingPreprocessor {

    // Remove tabs/newlines so the TSV structure stays clean
    private static String clean(String value) {
        if (value == null) {
            return "";
        }

        return value
                .replace("\t", " ")
                .replace("\r", " ")
                .replace("\n", " ")
                .trim();
    }

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println(
                "Usage: HotelBookingPreprocessor <input.csv> <output.tsv>"
            );
            return;
        }

        String inputFile = args[0];
        String outputFile = args[1];

        long totalRows = 0;
        long validRows = 0;
        long skippedRows = 0;
        long missingCountry = 0;

        try (
            Reader reader = Files.newBufferedReader(
                Paths.get(inputFile),
                StandardCharsets.UTF_8
            );

            CSVParser parser = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withIgnoreHeaderCase()
                .withTrim()
                .parse(reader);

            BufferedWriter writer = Files.newBufferedWriter(
                Paths.get(outputFile),
                StandardCharsets.UTF_8
            )
        ) {

            for (CSVRecord record : parser) {

                totalRows++;

                try {
                    String hotel =
                        clean(record.get("hotel"));

                    String isCanceled =
                        clean(record.get("is_canceled"));

                    String leadTime =
                        clean(record.get("lead_time"));

                    String arrivalYear =
                        clean(record.get("arrival_date_year"));

                    String arrivalMonth =
                        clean(record.get("arrival_date_month"));

                    String weekendNights =
                        clean(record.get("stays_in_weekend_nights"));

                    String weekNights =
                        clean(record.get("stays_in_week_nights"));

                    String adults =
                        clean(record.get("adults"));

                    String country =
                        clean(record.get("country"));

                    String marketSegment =
                        clean(record.get("market_segment"));

                    String distributionChannel =
                        clean(record.get("distribution_channel"));

                    String repeatedGuest =
                        clean(record.get("is_repeated_guest"));

                    String depositType =
                        clean(record.get("deposit_type"));

                    String customerType =
                        clean(record.get("customer_type"));

                    String adr =
                        clean(record.get("adr"));

                    String reservationStatus =
                        clean(record.get("reservation_status"));

                    String reservationStatusDate =
                        clean(record.get("reservation_status_date"));

                    // Validate important numeric fields
                    Integer.parseInt(isCanceled);
                    Integer.parseInt(leadTime);
                    Integer.parseInt(arrivalYear);
                    Integer.parseInt(weekendNights);
                    Integer.parseInt(weekNights);
                    Integer.parseInt(adults);
                    Integer.parseInt(repeatedGuest);
                    Double.parseDouble(adr);

                    // Replace missing country with UNKNOWN
                    if (country.isEmpty()
                            || country.equalsIgnoreCase("NULL")) {

                        country = "UNKNOWN";
                        missingCountry++;
                    }

                    // Write cleaned TSV row
                    writer.write(
                        hotel + "\t" +
                        isCanceled + "\t" +
                        leadTime + "\t" +
                        arrivalYear + "\t" +
                        arrivalMonth + "\t" +
                        weekendNights + "\t" +
                        weekNights + "\t" +
                        adults + "\t" +
                        country + "\t" +
                        marketSegment + "\t" +
                        distributionChannel + "\t" +
                        repeatedGuest + "\t" +
                        depositType + "\t" +
                        customerType + "\t" +
                        adr + "\t" +
                        reservationStatus + "\t" +
                        reservationStatusDate
                    );

                    writer.newLine();
                    validRows++;

                } catch (Exception e) {
                    skippedRows++;
                }
            }

            System.out.println();
            System.out.println("==============================================");
            System.out.println(" HOTEL BOOKING PREPROCESSING COMPLETE");
            System.out.println("==============================================");
            System.out.println("Total raw rows  : " + totalRows);
            System.out.println("Valid rows      : " + validRows);
            System.out.println("Skipped rows    : " + skippedRows);
            System.out.println("Missing country : " + missingCountry);
            System.out.println("Output file     : " + outputFile);
            System.out.println("==============================================");

        } catch (Exception e) {
            System.out.println("Preprocessing failed.");
            e.printStackTrace();
        }
    }
}