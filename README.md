\# Hotel Booking Data Analysis using Hadoop



A Big Data Analytics Lab project that analyzes the Hotel Booking Demand dataset using Hadoop MapReduce and Apache Pig.



\## Dataset



\- Dataset: Hotel Booking Demand

\- Total records: 119,390

\- Raw format: CSV

\- Processed format: TSV

\- Cleaned records: 119,390



\## Technologies Used



\- Java 8

\- Hadoop 3.2.4

\- HDFS

\- YARN

\- Apache Pig 0.18.0

\- Apache Commons CSV

\- Maven



\## Data Preprocessing



The original `hotel\_bookings.csv` dataset was preprocessed using Java and Apache Commons CSV.



The cleaned dataset contains 17 selected fields and is stored as:



`data/processed/Hotel\_Booking\_Clean.tsv`



The cleaned dataset was uploaded to HDFS at:



`/user/DELL/hotel\_project/input/Hotel\_Booking\_Clean.tsv`



\## MapReduce Analyses



1\. Top 10 Countries by Number of Bookings

2\. Cancellations by Market Segment

3\. Top 10 Countries by Total Stayed Nights



\## Apache Pig Analyses



1\. Top 5 Countries by Booking Count

2\. Top 10 Countries by Average ADR

3\. Top 10 Countries by Number of Cancellations

4\. Market Segments by Booking Count

5\. Top 10 Arrival Months by Booking Count



\## Key Results



\### Top Country by Booking Count

Portugal (PRT) - 48,590 bookings



\### Market Segment with Most Cancellations

Online TA - 20,739 cancellations



\### Country with Highest Total Stayed Nights

Portugal (PRT) - 52,857 nights



\### Month with Highest Booking Count

August - 13,877 bookings



\## Project Structure



```text

Hotel-Booking-Data-Analysis-using-Hadoop/

|

|-- data/

|   |-- raw/

|   `-- processed/

|

|-- Preprocessing/

|

|-- Top10Countries/

|

|-- CancellationByMarketSegment/

|

|-- Top10CountriesByStayedNights/

|

|-- PigAnalysis/

|   |-- scripts/

|   `-- outputFiles/

|

|-- screenshots/

|

|-- .gitignore

`-- README.md

