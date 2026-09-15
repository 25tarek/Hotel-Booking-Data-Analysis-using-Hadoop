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

valid = FILTER data BY country IS NOT NULL
    AND country != ''
    AND country != 'UNKNOWN';

grouped = GROUP valid BY country;

counts = FOREACH grouped GENERATE
    group AS country,
    COUNT(valid) AS booking_count;

sorted = ORDER counts BY booking_count DESC;

top5 = LIMIT sorted 5;

STORE top5
INTO '/user/DELL/hotel_project/pig_output/top5_countries'
USING PigStorage('\t');