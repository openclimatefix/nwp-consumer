# MetOffice API

---


## Data

Currently being fetched from our MetOffice orders:

### `uk-5params-35steps`

| Name                              | Long Name                                          | Level        | ID        | Unit   |
|-----------------------------------|----------------------------------------------------|--------------|-----------|--------|
| Low Cloud Cover                   | low-cloud-cover                                    | `atmosphere` | `lcc`     | %      |
| Snow Depth                        | snow-depth-water-equivalent                        | `ground`     | `sd`      | kg m-2 |
| Downward Shortwave Radiation Flux | downward-short-wave-radiation-flux                 | `ground`     | `dswrf`   | W m-2  |
| Temperature at 1.5m               | temperature                                        | `agl`        | `t2m`     | K      |
| Wind Direction at 10m             | wind-direction-from-which-blowing-surface-adjusted | `agl`        | `unknown` |        |

### `uk-11params-12steps`

| Name                                 | Long Name                          | Level        | ID        | Unit       |
|--------------------------------------|------------------------------------|--------------|-----------|------------|
| High Cloud Cover                     | high-cloud-cover                   | `atmosphere` | `hcc`     | %          |
| Medium Cloud Cover                   | medium-cloud-cover                 | `atmosphere` | `mcc`     | %          |
| Low Cloud Cover                      | low-cloud-cover                    | `atmosphere` | `lcc`     | %          |
| Visibility at 1.5m                   | visibility                         | `agl`        | `vis`     | m          |
| Relative Humidity at 1.5m            | relative-humidity                  | `agl`        | `r2`      | %          |
| Rain Precipitation Rate              | rain-precipitation-rate            | `ground`     | `rprate`  | kg m-2 s-1 |
| Snow Depth - ground                  | snow-depth-water-equivalent        | `ground`     | `sd`      | kg m-2     |
| Downward Longwave Radiation Flux     | downward-long-wave-radiation-flux  | `ground`     | `dlwrf`   | W m-2      |
| Downward Shortwave Radiation Flux    | downward-short-wave-radiation-flux | `ground`     | `dswrf`   | W m-2      |
| Temperature at 1.5m                  | temperature                        | `agl`        | `t2m`     | K          |
| Wind Speed at 10m (Surface Adjusted) | wind-speed-surface-adjusted        | `agl`        | `unknown` | m s-1      |

> :warning: **NOTE:** The two wind parameters are read in from their grib files as "unknown"    

## Parameter names in datasets

These orders may provide multiple time steps per "latest" file list.

Each parameter is loaded as a separate grib file.

<details>
  <summary>Datasets</summary>

    --- relative-humidity-1.5 ---
    Dimensions:            (step: 10, y: 639, x: 455)
    Coordinates:
        time               datetime64[ns] 2023-03-08T10:00:00
      * step               (step) timedelta64[ns] 00:00:00 01:00:00 ... 12:00:00
        heightAboveGround  float64 1.5
        latitude           (y, x) float64 ...
        longitude          (y, x) float64 ...
        valid_time         (step) datetime64[ns] ...
    Dimensions without coordinates: y, x
    Data variables:
        r2                 (step, y, x) float32 ...

    --- temperature 1.5m ---
    Dimensions:            (step: 10, y: 639, x: 455)
    Coordinates:
        time               datetime64[ns] 2023-03-08T10:00:00
      * step               (step) timedelta64[ns] 00:00:00 01:00:00 ... 12:00:00
        heightAboveGround  float64 1.5
        latitude           (y, x) float64 ...
        longitude          (y, x) float64 ...
        valid_time         (step) datetime64[ns] ...
    Dimensions without coordinates: y, x
    Data variables:
        t2m                (step, y, x) float32 ... (t2m because it's called "temperature 2m", even though it's at 1.5m)

    --- visibility 1.5 ---
    Dimensions:            (step: 10, y: 639, x: 455)
    Coordinates:
        time               datetime64[ns] 2023-03-08T10:00:00
      * step               (step) timedelta64[ns] 00:00:00 01:00:00 ... 12:00:00
        heightAboveGround  float64 1.5
        latitude           (y, x) float64 ...
        longitude          (y, x) float64 ...
        valid_time         (step) datetime64[ns] ...
    Dimensions without coordinates: y, x
    Data variables:
        vis                (step, y, x) float32 ...

    --- wind speed surface adjusted ---
    Dimensions:            (step: 10, y: 639, x: 455)
    Coordinates:
        time               datetime64[ns] 2023-03-08T10:00:00
      * step               (step) timedelta64[ns] 00:00:00 01:00:00 ... 12:00:00
        heightAboveGround  float64 10.0
        latitude           (y, x) float64 ...
        longitude          (y, x) float64 ...
        valid_time         (step) datetime64[ns] ...
    Dimensions without coordinates: y, x
    Data variables:
        unknown            (step, y, x) float32 ...

    --- high cloud cover ---
    Dimensions:     (step: 10, y: 639, x: 455)
    Coordinates:
        time        datetime64[ns] 2023-03-08T10:00:00
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 08:00:00 12:00:00
        atmosphere  float64 0.0
        latitude    (y, x) float64 ...
        longitude   (y, x) float64 ...
        valid_time  (step) datetime64[ns] ...
    Dimensions without coordinates: y, x
    Data variables:
        hcc         (step, y, x) float32 ...

    --- low cloud cover ---
    Coordinates:
        time        datetime64[ns] 2023-03-08T10:00:00
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 08:00:00 12:00:00
        atmosphere  float64 0.0
        latitude    (y, x) float64 ...
        longitude   (y, x) float64 ...
        valid_time  (step) datetime64[ns] ...
    Dimensions without coordinates: y, x
    Data variables:
        lcc         (step, y, x) float32 ...

    --- medium cloud cover ---
    Dimensions:     (step: 10, y: 639, x: 455)
    Coordinates:
        time        datetime64[ns] 2023-03-08T10:00:00
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 08:00:00 12:00:00
        atmosphere  float64 0.0
        latitude    (y, x) float64 ...
        longitude   (y, x) float64 ...
        valid_time  (step) datetime64[ns] ...
    Dimensions without coordinates: y, x
    Data variables:
        mcc         (step, y, x) float32 ...

    --- downward longwave radiation flux ---
    Dimensions:     (step: 10, y: 639, x: 455)
    Coordinates:
        time        datetime64[ns] 2023-03-08T10:00:00
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 08:00:00 12:00:00
        surface     float64 0.0
        latitude    (y, x) float64 ...
        longitude   (y, x) float64 ...
        valid_time  (step) datetime64[ns] ...
    Dimensions without coordinates: y, x
    Data variables:
        dlwrf       (step, y, x) float32 ...

    --- downward shortwave radiation flux ---
    Dimensions:     (step: 10, y: 639, x: 455)
    Coordinates:
        time        datetime64[ns] 2023-03-08T10:00:00
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 08:00:00 12:00:00
        surface     float64 0.0
        latitude    (y, x) float64 ...
        longitude   (y, x) float64 ...
        valid_time  (step) datetime64[ns] ...
    Dimensions without coordinates: y, x
    Data variables:
        dswrf       (step, y, x) float32 ...

    --- snow depth ---
    Dimensions:     (step: 10, y: 639, x: 455)
    Coordinates:
        time        datetime64[ns] 2023-03-08T10:00:00
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 08:00:00 12:00:00
        surface     float64 0.0
        latitude    (y, x) float64 ...
        longitude   (y, x) float64 ...
        valid_time  (step) datetime64[ns] ...
    Dimensions without coordinates: y, x
    Data variables:
        sd          (step, y, x) float32 ...

    --- rain precipitation rate ---
    Dimensions:     (step: 10, y: 639, x: 455)
    Coordinates:
        time        datetime64[ns] 2023-03-08T21:00:00
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 08:00:00 12:00:00
        surface     float64 0.0
        latitude    (y, x) float64 ...
        longitude   (y, x) float64 ...
        valid_time  (step) datetime64[ns] ...
    Dimensions without coordinates: y, x
    Data variables:
        rprate      (step, y, x) float32 ...

    --- wind direction from which blowing surface adjusted ---
    Dimensions:            (step: 36, y: 639, x: 455)
    Coordinates:
        time               datetime64[ns] 2023-03-08T21:00:00
      * step               (step) timedelta64[ns] 00:00:00 ... 1 days 11:00:00
        heightAboveGround  float64 10.0
        latitude           (y, x) float64 ...
        longitude          (y, x) float64 ...
        valid_time         (step) datetime64[ns] ...
    Dimensions without coordinates: y, x
    Data variables:
        unknown            (step, y, x) float32 ...

</details>
