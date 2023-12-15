"""Defines all parameters available from icon."""


EU_SL_VARS = [
    "alb_rad", "alhfl_s", "ashfl_s", "asob_s", "asob_t",
    "aswdifd_s", "aswdifu_s", "aswdir_s", "athb_s",
    "athb_t", "aumfl_s" "avmfl_s",
    "cape_con", "cape_ml", "clch", "clcl", "clcm", "clct", "clct_mod", "cldepth",
    "h_snow", "hbas_con", "htop_con", "htop_dc", "hzerocl",
    "pmsl", "ps",
    "qv_2m", "qv_s",
    "rain_con", "rain_gsp", "relhum_2m", "rho_snow", "runoff_g", "runoff_s",
    "snow_con", "snow_gsp", "snowlmt", "synmsg_bt_cl_ir10.8",
    "t_2m", "t_g", "t_snow", "tch", "tcm", "td_2m",
    "tmax_2m", "tmin_2m", "tot_prec", "tqc", "tqi",
    "u_10m", "v_10m", "vmax_10m",
    "w_snow", "w_so", "ww",
    "z0",
]

EU_ML_VARS = ["clc", "fi", "omega", "p", "qv", "relhum", "t", "tke", "u", "v", "w"]

GLOBAL_SL_VARS = [
    *EU_SL_VARS,
    "alb_rad",
    "c_t_lk",
    "freshsnw", "fr_ice",
    "h_ice", "h_ml_lk",
    "t_ice", "t_s", "tqr", "tqs", "tqv",
]

GLOBAL_ML_VARS: list[str] = ["fi", "relhum", "t", "u", "v"]

