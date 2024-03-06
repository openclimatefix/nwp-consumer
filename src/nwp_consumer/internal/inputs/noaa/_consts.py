"""Defines all parameters available from NOAA."""

GFS_VARIABLES = ['siconc_surface_instant', 'slt_surface_instant', 'cape_surface_instant', 't_surface_instant',
                 'sp_surface_instant', 'lsm_surface_instant', 'sr_surface_instant', 'vis_surface_instant',
                 'prate_surface_instant', 'acpcp_surface_accum', 'sde_surface_instant', 'cin_surface_instant',
                 'orog_surface_instant', 'tp_surface_accum', 'lhtfl_surface_avg', 'shtfl_surface_avg',
                 'crain_surface_instant', 'cfrzr_surface_instant', 'cicep_surface_instant', 'csnow_surface_instant',
                 'cprat_surface_instant', 'cpofp_surface_instant', 'pevpr_surface_instant', 'sdwe_surface_instant',
                 'uflx_surface_avg', 'vflx_surface_avg', 'gust_surface_instant', 'fricv_surface_instant',
                 'u-gwd_surface_avg', 'v-gwd_surface_avg', 'hpbl_surface_instant', 'dswrf_surface_avg',
                 'uswrf_surface_avg', 'dlwrf_surface_avg', 'ulwrf_surface_avg', 'lftx_surface_instant',
                 '4lftx_surface_instant', 'veg_surface_instant', 'watr_surface_accum', 'gflux_surface_avg',
                 'fco2rec_surface_instant', 'hindex_surface_instant', 'wilt_surface_instant', 'fldcp_surface_instant',
                 'al_surface_avg', 'SUNSD_surface_instant', 'prate_surface_avg', 'crain_surface_avg',
                 'cfrzr_surface_avg', 'cicep_surface_avg', 'csnow_surface_avg', 'cprat_surface_avg', 'pres_instant',
                 'q_instant', 't_instant', 'u_instant', 'v_instant', 'u10_instant', 'v10_instant', 't2m_instant',
                 'd2m_instant', 'tmax_max', 'tmin_min', 'sh2_instant', 'r2_instant', 'aptmp_instant', 'u100_instant',
                 'v100_instant', 'refd_instant', 't', 'u', 'v', 'q', 'w', 'gh', 'r', 'absv', 'o3mr', 'wz', 'tcc',
                 'clwmr', 'icmr', 'rwmr', 'snmr', 'grle', ]

MISSING_STEP_0_VARIABLES = ['slt_surface_instant', 'sr_surface_instant', 'acpcp_surface_accum', 'tp_surface_accum',
                            'lhtfl_surface_avg', 'shtfl_surface_avg', 'cprat_surface_instant', 'pevpr_surface_instant',
                            'uflx_surface_avg', 'vflx_surface_avg', 'fricv_surface_instant', 'u-gwd_surface_avg',
                            'v-gwd_surface_avg', 'dswrf_surface_avg', 'uswrf_surface_avg', 'dlwrf_surface_avg',
                            'ulwrf_surface_avg', 'veg_surface_instant', 'watr_surface_accum', 'gflux_surface_avg',
                            'fco2rec_surface_instant', 'al_surface_avg', 'prate_surface_avg', 'crain_surface_avg',
                            'cfrzr_surface_avg', 'cicep_surface_avg', 'csnow_surface_avg', 'cprat_surface_avg',
                            'tmax_max', 'tmin_min', 'refd_instant', 'q', ]

EXTRA_STEP_0_VARIABLES = ["landn_surface_instant", "5wavh"]
