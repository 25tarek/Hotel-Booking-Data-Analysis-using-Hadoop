data = LOAD '/user/DELL/hotel_project/input/Hotel_Booking_Clean.tsv'
USING PigStorage('\t')
AS (
    hotel:chararray,
    is_canceled:int,
    lead_time:int,
    arrival_date_year:int,
    arrival_date_month:chararray,
    stays_in_weekend_nights:int,
    stays_in_week_nights:int,
    adults:int,
    country:chararray,
    market_segment:chararray,
    distribution_channel:chararray,
    is_repeated_guest:int,
    deposit_type:chararray,
    customer_type:chararray,
    adr:double,
    reservation_status:chararray,
    reservation_status_date:chararray
);

valid = FILTER data BY
    market_segment IS NOT NULL
    AND market_segment != '';

grouped = GROUP valid BY market_segment;

counts = FOREACH grouped GENERATE
    group AS market_segment,
    COUNT(valid) AS booking_count;

sorted = ORDER counts BY booking_count DESC;

top10 = LIMIT sorted 10;

STORE top10
INTO '/user/DELL/hotel_project/pig_output/top10_market_segments'
USING PigStorage('\t');