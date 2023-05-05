# CEDA

---

## Data

See
- https://artefacts.ceda.ac.uk/formats/grib/
- https://dap.ceda.ac.uk/badc/ukmo-nwp/doc/NWP_UKV_Information.pdf

Investigate files via eccodes:

```shell
$ conda install -c conda-forge eccodes
```

More info on eccodes: https://confluence.ecmwf.int/display/ECC/grib_ls

For example:

```shell
$ grib_ls -n parameter -w stepRange=1 201901010000_u1096_ng_umqv_Wholesale1.grib
```

## Files

Sourced from https://zenodo.org/record/7357056. There are two files per 
`init_time` (model run time) that contain surface-level parameters of interest.

The contents of those files differs somewhat from what is presented in the above
document

#### Un-split File 1 `yyyymmddhhmm_u1096_ng_umqv_Wholesale1.grib`

Full domain, 35 time steps and the following surface level parameters.

| paramId | shortName | units          | name                    |
|---------|-----------|----------------|-------------------------|
| 130     | t         | K              | Temperature             |
| 3017    | dpt       | K              | Dew point temperature   |
| 3020    | vis       | m              | Visibility              |
| 157     | r         | %              | Relative humidity       |
| 260074  | prmsl     | Pa             | Pressure reduced to MSL |
| 207     | 10si      | m s**-1        | 10 metre wind speed     |
| 260260  | 10wdir    | Degree true    | 10 metre wind direction |
| 3059    | prate     | kg m**-2 s**-1 | Precipitation rate      |
|         | unknown   | unknown        | unknown                 |

View via pasting the output of the following to this 
[online table converter](https://tableconvert.com/json-to-markdown):

```shell
$ grib_ls -n parameter -w stepRange=0 -j 201901010000_u1096_ng_umqv_Wholesale1.grib
```

When loading this file in using *cfgrib*, it loads in 5 distinct xarray datasets.

<details>
  <summary>Wholesale1 Datasets</summary>
    
    --- Dataset 1 ---
    Dimensions:            (step: 37, values: 385792)
    Coordinates:
        time               datetime64[ns] 2019-01-01
      * step               (step) timedelta64[ns] 00:00:00 ... 1 days 12:00:00
        heightAboveGround  float64 1.0
        valid_time         (step) datetime64[ns] 2019-01-01 ... 2019-01-02T12:00:00
    Dimensions without coordinates: values
    Data variables:
        t                  (step, values) float32 ... (1.5m temperature)
        r                  (step, values) float32 ... (1.5m relative humidity)
        dpt                (step, values) float32 ... (1.5m dew point)
        vis                (step, values) float32 ... (1.5m visibility)

    --- Dataset 2 ---
    Dimensions:            (step: 37, values: 385792)
    Coordinates:
        time               datetime64[ns] 2019-01-01
      * step               (step) timedelta64[ns] 00:00:00 ... 1 days 12:00:00
        heightAboveGround  float64 10.0
        valid_time         (step) datetime64[ns] 2019-01-01 ... 2019-01-02T12:00:00
    Dimensions without coordinates: values
    Data variables:
        si10               (step, values) float32 ... (10m wind speed)
        wdir10             (step, values) float32 ... (10m wind direction)
    
    --- Dataset 3 ---
    Dataset 3
    Dimensions:     (step: 37, values: 385792)
    Coordinates:
        time        datetime64[ns] 2019-01-01
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 1 days 12:00:00
        meanSea     float64 0.0
        valid_time  (step) datetime64[ns] ...
    Dimensions without coordinates: values
    Data variables:
        prmsl       (step, values) float32 ... (mean sea level pressure)
    
    --- Dataset 4 ---
    Dimensions:     (step: 36, values: 385792)
    Coordinates:
        time        datetime64[ns] 2019-01-01
      * step        (step) timedelta64[ns] 01:00:00 02:00:00 ... 1 days 12:00:00
        surface     float64 0.0
        valid_time  (step) datetime64[ns] ...
    Dimensions without coordinates: values
    Data variables:
        unknown     (step, values) float32 ... (?)
    
    --- Dataset 5 ---
    Dimensions:     (step: 37, values: 385792)
    Coordinates:
        time        datetime64[ns] 2019-01-01
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 1 days 12:00:00
        surface     float64 0.0
        valid_time  (step) datetime64[ns] 2019-01-01 ... 2019-01-02T12:00:00
    Dimensions without coordinates: values
    Data variables:
        unknown     (step, values) float32 ... (?)
        prate       (step, values) float32 ... (total precipitation rate)

</details>

#### Un-split File 2 `yyyymmddhhmm_u1096_ng_umqv_Wholesale2.grib`

Full domain, 35 time steps and the following surface level parameters:

| centre | paramId | shortName | units   | name                               |
|--------|---------|-----------|---------|------------------------------------|
| egrr   |         | unknown   | unknown | unknown                            |
| egrr   | 3073    | lcc       | %       | Low cloud cover                    |
| egrr   | 3074    | mcc       | %       | Medium cloud cover                 |
| egrr   | 3075    | hcc       | %       | High cloud cover                   |
| egrr   |         | unknown   | unknown | unknown                            |
| egrr   | 228046  | hcct      | m       | Height of convective cloud top     |
| egrr   | 3073    | lcc       | %       | Low cloud cover                    |
| egrr   | 260107  | cdcb      | m       | Cloud base                         |
| egrr   | 3066    | sde       | m       | Snow depth                         |
| egrr   | 260087  | dswrf     | W m**-2 | Downward short-wave radiation flux |
| egrr   | 260097  | dlwrf     | W m**-2 | Downward long-wave radiation flux  |
| egrr   |         | unknown   | unknown | unknown                            |
| egrr   | 3008    | h         | m       | Geometrical height                 |

View via pasting the ouput of the following to this 
[online table converter](https://tableconvert.com/json-to-markdown):

```shell
$ grib_ls -n parameter -w stepRange=0 -j 201901010000_u1096_ng_umqv_Wholesale2.grib
```

When loading this file to xarray using *cfgrib*, it comes in 6 distinct
datasets. These datasets only contain 11 of the 13 parameters specified
above, with two of the 11 being unknown variables.

<details>
  <summary>Wholesal21 Datasets</summary>

    --- Dataset 1 ---
    Dimensions:     (step: 37, values: 385792)
    Coordinates:
        time        datetime64[ns] 2019-01-01
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 1 days 12:00:00
        atmosphere  float64 0.0
        valid_time  (step) datetime64[ns] ...
    Dimensions without coordinates: values
    Data variables:
        unknown     (step, values) float32 ... (?)
    
    --- Dataset 2 ---
    Dimensions:     (step: 37, values: 385792)
    Coordinates:
        time        datetime64[ns] 2019-01-01
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 1 days 12:00:00
        cloudBase   float64 0.0
        valid_time  (step) datetime64[ns] ...
    Dimensions without coordinates: values
    Data variables:
        cdcb        (step, values) float32 ... (convective cloud base height)
    
    --- Dataset 3 ---
    Dimensions:                 (step: 37, values: 385792)
    Coordinates:
        time                    datetime64[ns] 2019-01-01
      * step                    (step) timedelta64[ns] 00:00:00 ... 1 days 12:00:00
        heightAboveGroundLayer  float64 0.0
        valid_time              (step) datetime64[ns] ...
    Dimensions without coordinates: values
    Data variables:
        lcc                     (step, values) float32 ... (low cloud amount)
    
    --- Dataset 4 ---
    Dimensions:                 (step: 37, values: 385792)
    Coordinates:
        time                    datetime64[ns] 2019-01-01
      * step                    (step) timedelta64[ns] 00:00:00 ... 1 days 12:00:00
        heightAboveGroundLayer  float64 1.524e+03
        valid_time              (step) datetime64[ns] ...
    Dimensions without coordinates: values
    Data variables:
        mcc                     (step, values) float32 ... (medium cloud amount)
    
    --- Dataset 5 ---
    Dimensions:                 (step: 37, values: 385792)
    Coordinates:
        time                    datetime64[ns] 2019-01-01
      * step                    (step) timedelta64[ns] 00:00:00 ... 1 days 12:00:00
        heightAboveGroundLayer  float64 4.572e+03
        valid_time              (step) datetime64[ns] ...
    Dimensions without coordinates: values
    Data variables:
        hcc                     (step, values) float32 ... (high cloud amount)
    
    --- Dataset 6 ---
    Dimensions:     (step: 37, values: 385792)
    Coordinates:
        time        datetime64[ns] 2019-01-01
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 1 days 12:00:00
        surface     float64 0.0
        valid_time  (step) datetime64[ns] 2019-01-01 ... 2019-01-02T12:00:00
    Dimensions without coordinates: values
    Data variables:
        unknown     (step, values) float32 ...
        sde         (step, values) float32 ... (snow depth water equivalent)
        hcct        (step, values) float32 ... (height of convective cloud top)
        dswrf       (step, values) float32 ... (downward short-wave radiation flux)
        dlwrf       (step, values) float32 ... (downward long-wave radiation flux)
    
    --- Dataset 7 ---
    Dimensions:     (step: 37, values: 385792)
    Coordinates:
        time        datetime64[ns] 2019-01-01
      * step        (step) timedelta64[ns] 00:00:00 01:00:00 ... 1 days 12:00:00
        level       float64 0.0
        valid_time  (step) datetime64[ns] ...
    Dimensions without coordinates: values
    Data variables:
        h           (step, values) float32 ... (geometrical height)

</details>


## Geography


The geography namespace of the files returns the following information:

```shell
grib_ls -n geography -w shortName=t,stepRange=0 -j 201901010000_u1096_ng_umqv_Wholesale1.grib
```


| Name                               | Value               | 
|------------------------------------|---------------------|
| Ni                                 | 548                 |
| Nj                                 | 704                 |
| latitudeOfReferencePointInDegrees  | 4.9e-05             |
| longitudeOfReferencePointInDegrees | -2e-06              |
| m                                  | 0                   |
| XRInMetres                         | 400000              |
| YRInMetres                         | -100000             |
| iScansNegatively                   | 0                   |
| jScansPositively                   | 1                   |
| jPointsAreConsecutive              | 0                   |
| DiInMetres                         | 2000                |
| DjInMetres                         | 2000                |
| X1InGridLengths                    | -238000             |
| Y1InGridLengths                    | 1.222e+06           |
| X2InGridLengths                    | 856000              |
| Y2InGridLengths                    | -184000             |
| gridType                           | transverse_mercator |
| bitmapPresent                      | 1                   |
| bitmap                             | 255...              |

