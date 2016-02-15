srb= load '$ASD_SE.asd_svcrqst' using org.apache.hcatalog.pig.HCatLoader();
cds_srb= load '$RES.asd_svcrqst_svcrqct_cds_srb' using org.apache.hcatalog.pig.HCatLoader();
claim_srb= load '$RES.ccr_claim_re_svcrqst_srb' using org.apache.hcatalog.pig.HCatLoader();
ros_map= load '$RES.rosetta_mapping_lookup_asd_srb' using org.apache.hcatalog.pig.HCatLoader();

-- Since all the prerequist have been completed we directly get into the join 

srb_ros_jn = join srb by (svcrtyp_cd, svrstyp_cd) left outer, ros_map by (i_srtp_sr_typ_cd, i_srtp_sr_sbtyp_cd) using 'replicated';
srb_ros_cds_jn= join srb_ros_jn by srb::svcrqst_id left outer, cds_srb by svcrqst_id; 
srb_ros_cds_clm_jn= join srb_ros_cds_jn by srb_ros_jn::srb::svcrqst_id left outer, claim_srb by svcrqst_id;

-- Generate the table values here after the join using disambiguate variables
srb_stageb_tab = foreach srb_ros_cds_clm_jn 
generate srb_ros_cds_jn::srb_ros_jn::srb::svcrqst_id as svcrqst_id, 
srb_ros_cds_jn::srb_ros_jn::srb::svcrqst_lupdusr_id as svcrqst_lupdusr_id, 
srb_ros_cds_jn::srb_ros_jn::srb::svcrqst_lstupd_dts as svcrqst_lupdt,
claim_srb::crsr_lupdt as crsr_lupdt,
srb_ros_cds_jn::srb_ros_jn::srb::svcrqst_crt_dts as svcrqst_crt_dts,
srb_ros_cds_jn::srb_ros_jn::srb::svcrqst_asrqst_ind as svcrqst_asrqst_ind,
srb_ros_cds_jn::srb_ros_jn::srb::svcrtyp_cd as svcrtyp_cd,
srb_ros_cds_jn::srb_ros_jn::srb::svrstyp_cd as svrstyp_cd,
srb_ros_cds_jn::srb_ros_jn::srb::asdplnsp_psuniq_id as psuniq_id,
srb_ros_cds_jn::srb_ros_jn::srb::svcrqst_rtnorig_in as svcrqst_rtnorig_in,
srb_ros_cds_jn::srb_ros_jn::srb::cmpltyp_cd as cmpltyp_cd,
srb_ros_cds_jn::srb_ros_jn::srb::catsrsn_cd as catsrsn_cd,
srb_ros_cds_jn::srb_ros_jn::srb::apealvl_cd as apealvl_cd,
srb_ros_cds_jn::srb_ros_jn::srb::cnstnty_cd as cnstnty_cd,
srb_ros_cds_jn::srb_ros_jn::srb::svcrqst_vwasof_dt as svcrqst_vwasof_dt,
claim_srb::crsr_master_claim_index as crsr_master_claim_index,
srb_ros_cds_jn::cds_srb::svcrqct_cds as svcrqct_cds,
srb_ros_cds_jn::srb_ros_jn::ros_map::sum_reason_cd as sum_reason_cd,
srb_ros_cds_jn::srb_ros_jn::ros_map::sum_reason as sum_reason;

-- Store the joined records into the final stageb table using HCatStorer
STORE srb_stageb_tab INTO '$RES.asd_stage_srb_tmp' USING org.apache.hive.hcatalog.pig.HCatStorer();
