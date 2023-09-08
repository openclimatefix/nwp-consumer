# ECMWF API

## Authentication

The ECMWF API requires the setting of a few environment variables,
or an `.ecmwfapirc` file in the user's home directory. See the PyPi entry:
https://pypi.org/project/ecmwf-api-client/, or the ECMWFMARSConfig class
in `nwp_consumer/internal/config/config.py`. The variables are

```shell
ECMWF_API_KEY=<your api key>
ECMWF_API_EMAIL=<your api email>
ECMWF_API_URL=<ecmwf api url>
```

which can be accessed via visiting [https://api.ecmwf.int/v1/key/](https://api.ecmwf.int/v1/key/).

## MARS

View the glossary for ECMWF MARS variables available for the operational forecast:
https://codes.ecmwf.int/grib/param-db

View the glossary for the MARS postprocessing keywords:
https://confluence.ecmwf.int/display/UDOC/Post-processing+keywords
