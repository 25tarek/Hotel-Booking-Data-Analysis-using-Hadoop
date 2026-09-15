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
    country IS NOT NULL
    AND country != ''
    AND country != 'UNKNOWN'
    AND is_canceled == 0
    AND adr IS NOT NULL
    AND adr > 0.0;

grouped = GROUP valid BY country;

averages = FOREACH grouped GENERATE
    group AS country,
    AVG(valid.adr) AS average_adr;

sorted = ORDER averages BY average_adr DESC;

top10 = LIMIT sorted 10;

STORE top10
INTO '/user/DELL/hotel_project/pig_output/top10_countries_average_adr'
USING PigStorage('\t');