
```{r tem-function}
# clean temperature
# expand the time range of temperature

name_df <- tribble(
  ~station_code, ~city_name, ~site_name,
  "9021", "PERTH", "PERTH AIRPORT",
  "18192", "PORT", "PORT LINCOLN AWS",
  "23090", "KENT", "KENT TOWN",
  "40842", "BRISBANE", "BRISBANE AERO",
  "66062", "SYDNEY", "SYDNEY (OBSERVATORY HILL)",
  "70351", "CANBERRA", "CANBERRA AIRPORT",
  "86338", "MELBOURNE", "MELBOURNE (OLYMPIC PARK)"
)

read_temp_list <- list.files(here::here("data", "2020-01-07")) %>%
  .[str_detect(., "IDCJAC0010|IDCJAC0011")]

read_clean_temp_data <- function(file_name){

  temp_descrip <- if_else(str_detect(file_name, "IDCJAC0010"), "max", "min")

  read_csv(here::here("data", "2020-01-07", file_name)) %>%
    janitor::clean_names() %>%
    rename(station_code = bureau_of_meteorology_station_number,
           temperature = contains("degree")) %>%
    mutate(temp_type = temp_descrip,
           date = paste0(year,"-",month,"-",day),
           station_code = as.character(station_code)) %>%
    left_join(name_df, by = "station_code") %>%
    select(city_name, date, temperature, temp_type, site_name)

}
```


```{r tem-expand, message=FALSE}
# map raw data
clean_temp_df <- read_temp_list %>%
  purrr::map(read_clean_temp_data) %>%
  bind_rows() %>%
  mutate(date = as.Date(date, "%Y-%m-%d"))%>%
  filter(date > "2019-05-31" & date <= "2020-01-05")
```

```{r rain-b-function}
# expand the time range of rainfall for Brisbane
name_df_b <- tribble(
  ~station_code, ~city_name, ~lat, ~long, ~station_name,
  "40140", "Brisbane", -27.48, 153.04, "Brisbane",
)

read_precip_list <- list.files(here::here("data", "2020-01-07")) %>%
  .[str_detect(., "IDCJAC0009")]

read_clean_precip_data_b <- function(file_name){

  read_csv(here::here("data", "2020-01-07", file_name)) %>%
    janitor::clean_names() %>%
    select("station_code" = bureau_of_meteorology_station_number,
           year, month, day,
           "rainfall"= rainfall_amount_millimetres,
           "period" = period_over_which_rainfall_was_measured_days,
           quality) %>%
    mutate(station_code = as.character(station_code),
           date = paste0(year,"-",month,"-",day)) %>%
    left_join(name_df_b, by = "station_code") %>%
    select(station_code, city_name, everything())
}
```


```{r rain-c-function}
# expand the time range of rainfall for Canberra
name_df_c <- tribble(
  ~station_code, ~city_name, ~lat, ~long, ~station_name,
  "70246", "Canberra", -35.31, 149.2, "Canberra Airport"
)

read_precip_list <- list.files(here::here("data", "2020-01-07")) %>%
  .[str_detect(., "IDCJAC0009")]

read_clean_precip_data_c <- function(file_name){

  read_csv(here::here("data", "2020-01-07", file_name)) %>%
    janitor::clean_names() %>%
    select("station_code" = bureau_of_meteorology_station_number,
           year, month, day,
           "rainfall"= rainfall_amount_millimetres,
           "period" = period_over_which_rainfall_was_measured_days,
           quality) %>%
    mutate(station_code = as.character(station_code),
           date = paste0(year,"-",month,"-",day)) %>%
    left_join(name_df_c, by = "station_code") %>%
    select(station_code, city_name, everything())
}
```

```{r rain-expand-canberra, message=FALSE}
# select the time range with Canberra data
clean_rain_df_canberra <- read_precip_list %>%
  purrr::map(read_clean_precip_data_c) %>%
  bind_rows()%>%
  mutate(date = as.Date(date, "%Y-%m-%d"))%>%
  filter(date <= "2008-09-18") %>%
  mutate(station_code=recode(station_code,
                             "70246"= "070351"))%>%
  select(-date)

```

```{r rain-expand-brisbane, message=FALSE}
# select the time range with brisbane data
clean_rain_df_brisbane <- read_precip_list %>%
  purrr::map(read_clean_precip_data_b) %>%
  bind_rows()%>%
  mutate(date = as.Date(date, "%Y-%m-%d"))%>%
  filter(date <= "1999-12-10")%>%
  mutate(station_code=recode(station_code,
                             "40140"="040913"))%>%
  select(-date)

```

```{r merge-data, message=FALSE, warning=FALSE}
# Get the cleaned data from Github
# the time range for old temperature data is from 1910-01-01 to 2019-05-31
rainfall_old <- readr::read_csv('https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-01-07/rainfall.csv')
temperature_old <- readr::read_csv('https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-01-07/temperature.csv')

# merge two temperature data sets to get final temperature data
temperature_new <- bind_rows(temperature_old, clean_temp_df)

# merge three rainfall data sets to get final rainfall data
rainfall_new<-bind_rows(rainfall_old,clean_rain_df_canberra,clean_rain_df_brisbane)

# write new temperature and rainfall data
write_csv(temperature_new, here::here("data","2020-01-07","temperature_new.csv"))
write_csv(rainfall_new, here::here("data", "2020-01-07","rainfall_new.csv"))

```

