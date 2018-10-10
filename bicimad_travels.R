library(rjson)
library(tidyverse)
library(ggmap)

parse_json <- function(dataline, get_day, station_hash) {
    jd <- fromJSON(dataline)
    # Not interested in travels without intermediate data points.
    if (length(jd$track$features) == 0) {
        return(NULL)
    }
    unplug_hourTime <- jd$unplug_hourTime[['$date']]
    if (!grepl(get_day, unplug_hourTime)) {
        return(NULL)
    }
    # We need to build an expanded data.frame with all the relevant rows.
    oid <- jd$`_id`[['$oid']]
    user_day_code <- jd$user_day_code
    user_type <- jd$user_type
    end_coords <- station_hash[[as.character(jd$idplug_station)]]
    start_coords <- station_hash[[as.character(jd$idunplug_station)]]
    travel_time <- jd$travel_time
    if (is.null(end_coords) || is.null(start_coords)) {
        # We don't have the coordinates for this station
        return(NULL)
    }
    
    initial_point <- data.frame(oid = oid,
                                user_day_code = user_day_code,
                                user_type = user_type,
                                unplug_hourTime = unplug_hourTime,
                                longitude = start_coords[1],
                                latitude = start_coords[2],
                                seconds_from_start = 0)
    end_point <- initial_point
    end_point$longitude <- end_coords[1]
    end_point$latitude <- end_coords[2]
    end_point$seconds_from_start <- travel_time
    
    df <- rbind(initial_point, end_point)

    if (length(jd$track$features) > 0) {
        df2 <- lapply(jd$track$features, function(x) {
            longitude <- x$geometry$coordinates[1]
            latitude <- x$geometry$coordinates[2]
            seconds_from_start <- x$properties$secondsfromstart
            return(data.frame(oid = oid,
                              user_day_code = user_day_code,
                              user_type = user_type,
                              unplug_hourTime = unplug_hourTime,
                              longitude = longitude,
                              latitude = latitude,
                              seconds_from_start = seconds_from_start))
        })
        df2 <- do.call(rbind, df2)
        df <- rbind(df, df2)
    }
    return(df)
}

# Parse travel data
bicimad_data <- "~/tmp/201709_Usage_Bicimad.json.gz"
station_data <- "~/tmp/Bicimad_Estacions_201808.json.gz"

# Create a hash table that links station ids to a (longitude, latitude) vector.
# We only need the first line from station_data to do this.
station_hash <- new.env()
first_line <- fromJSON(read_lines(station_data, n_max = 1))
tmp <- lapply(first_line$stations, function(x) {
    # Environment hash keys are characters
    station_hash[[as.character(x$id)]] <- as.numeric(c(x$longitude, x$latitude))
})

f_rlc <- function(lines, pos) {
    return(do.call(rbind, lapply(lines, parse_json, 
                                 get_day = "2017-09-24",
                                 station_hash = station_hash)))
}

if (!exists("dd_orig")) {
    dd_orig <- read_lines_chunked(bicimad_data, 
                             callback = DataFrameCallback$new(f_rlc),
                             chunk_size = 1000)
}

dd <- dd_orig %>%
    mutate(unplug_hourTime = as.character(unplug_hourTime)) %>%
    mutate(oid = as.character(oid)) %>%
    arrange(unplug_hourTime, oid, seconds_from_start) %>% 
    group_by(oid) %>%
    mutate(to_longitude = lead(longitude)) %>%
    mutate(to_latitude = lead(latitude)) %>%
    filter(!is.na(to_longitude))
    

# Get the map with coordinates by hand, make_bbox is not centering correctly.
mymap <- get_stamenmap(bbox = c(-3.75, 40.38, -3.64, 40.465), 
                       maptype = 'terrain', zoom = 13)

# We don't have the actual starting time for each trip, but let's suppose
# we know it. We can just pretend all the trips are uniformly distributed
# every hour (they are not, but just for plotting). Then we compute an 
# artificial "time" variable and plot the last N minutes with different 
# alpha levels. That will leave the imprint and will fade away after a certain
# number of frames. It's goood enough to see where which areas show movement
# at different times of the day. For this, we just need to assign every 
# starting trip (group per oid!) a number between 0 and 3600 (seconds in an 
# hour), and then plot in a certain window.

# Copy data just to not destroy what we already have.
dd2 <- dd %>% 
    group_by(unplug_hourTime, oid) %>% 
    summarise(tmp1 = n()) %>% 
    group_by(unplug_hourTime) %>% 
    mutate(idx = row_number()) %>%
    mutate(n_trips = max(idx)) %>%
    mutate(hour = as.numeric(strsplit(strsplit(unplug_hourTime, "T")[[1]][2], ":")[[1]][1])) %>%
    mutate(synth_time = 3600 * hour + 3600 * idx / n_trips) %>%
    ungroup() %>%
    left_join(dd) %>%
    group_by(oid) %>%
    mutate(relative_time = seconds_from_start + min(synth_time))
    

window_length <- 1800 # our window is 30 minutes
window_step <- 60 # one frame every minute
t_current <- 0
n_plot <- 0

# Create one map, just add to it at every frame
initial_map <- ggmap(mymap, darken = 0.7) +
    theme(legend.position = "bottom",
          axis.title.x = element_blank(),
          axis.text.x = element_blank(),
          axis.ticks.x = element_blank(),
          axis.title.y = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.y = element_blank(), 
          legend.key.width = unit(2, "cm"))

while (t_current <= max(dd2$relative_time)) {
    print(n_plot)
    dd_partial <- dd2[which(dd2$relative_time >= t_current - window_length &
                            dd2$relative_time <= t_current), ]
    t_current <- t_current + window_step
    max_rt <- max(dd_partial$relative_time)
    min_rt <- min(dd_partial$relative_time)
    max_time <- max(as.character(dd_partial$unplug_hourTime))
    if (is.na(max_time)) {
        max_time <- min(as.character(dd2$unplug_hourTime))
    }
    max_time <- paste(strsplit(max_time, "T")[[1]][1],
                      strsplit(strsplit(max_time, "T")[[1]][2], ".", 
                               fixed = TRUE)[[1]][1])
    
    dd_partial$alpha <- 0.2 * (dd_partial$relative_time - min_rt) / (max_rt - min_rt)
    plt1 <- initial_map + geom_segment(aes(x = longitude, y = latitude, 
                                           xend = to_longitude, yend = to_latitude,
                                           alpha = alpha,
                                           group = oid),
                                       color = "white", 
                                       data = dd_partial) +
        scale_alpha_continuous(guide = FALSE) + 
        # Yes, I found these coordinates by hand. I... I am sorry.
        geom_text(x = -3.73, y = 40.462, label = max_time,
                  size = 6, color = "white") +
        geom_text(x = -3.659, y = 40.383, label = "https://rinzewind.org", size = 5,
                  color = "grey", fontface = "italic") +
    
    ggsave(filename = sprintf("/tmp/bicimad_%04d.png", n_plot), dpi = 100)
    n_plot <- n_plot + 1
}
# Then: ffmpeg -framerate 10 -i bicimad_%04d.png -c:v libx264 out.mp4
