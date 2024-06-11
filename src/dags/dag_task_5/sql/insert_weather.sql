INSERT INTO weather (location, date, avg_temp_c, condition)
VALUES ('{{ ti.xcom_pull(key="formatted_weather_data")["location"] }}',
        '{{ ti.xcom_pull(key="formatted_weather_data")["date"] }}',
        {{ ti.xcom_pull(key="formatted_weather_data")["avg_temp_c"] }},
        '{{ ti.xcom_pull(key="formatted_weather_data")["condition"] }}');
