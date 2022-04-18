b2b_new_cust = {
	"material_type" : ['OPC PREMIUM', 'OPC REGULER','OWC','PCC PREMIUM', 'PCC', 'PPC', 'SBC','TYPE II', 'TYPE V'],
	"packaging_mode" :['bulk','jumbo bag'],
	"segmentsi" : ['cpm & kontraktor', 'beton rmx', 'bumn'],
	"cluster":['bronze', 'platinum', 'silver', 'gold']	
}

model_elasticity_retail = {
    "ST" : "model_gbt_nasional_st.pkl",
    "SG" : "model_gbt_nasional_sg.pkl",
    "DYNAMIX":"model_gbt_nasional_dynamix.pkl",
    "POWERMAX": "model_rf_nasional_powermax.pkl",
    "SP":"model_gbt_nasional_sp.pkl",
    "ANDALAS":"model_rf_nasional_andalas.pkl",
    "MASONRY":"model_rf_nasional_masonry.pkl"
}

model_pricing_retail = {
    "ST" : "GBT_pricing_brand_sig_st_model_retail.pkl",
    "SG" : "GBT_pricing_brand_sig_sg_model_retail.pkl",
    "DYNAMIX":"GBT_pricing_brand_sig_dynamix_model_retail.pkl",
    "POWERMAX": "GBT_pricing_brand_sig_powermax_model_retail.pkl",
    "SP":"GBT_pricing_brand_sig_sp_model_retail.pkl",
    "ANDALAS":"GBT_pricing_brand_sig_andalas_model_retail.pkl",
    "MASONRY":"GBT_pricing_brand_sig_masonry_model_retail.pkl"
}


model_elasticity_b2b = {
    "DURAMAX" : "elasticity_brand_sig_duramax_model_b2b_random_forest.pkl",
    "OPC Premium" : "elasticity_brand_sig_opc_premium_model_b2b_random_forest.pkl",
    "OPC Reguler":"elasticity_brand_sig_opc_reguler_model_b2b_random_forest.pkl",
    "OWC":"elasticity_brand_sig_owc_model_b2b_random_forest.pkl",
    "PCC":"elasticity_brand_sig_pcc_model_b2b_random_forest.pkl",
    "PCC Premium":"elasticity_brand_sig_pcc_premium_model_b2b_random_forest.pkl",
    "SBC":"elasticity_brand_sig_sbc_model_b2b_random_forest.pkl",
    "Type II":"elasticity_brand_sig_type_ii_model_b2b_random_forest.pkl",
    "Type V":"elasticity_brand_sig_type_v_model_b2b_random_forest.pkl"
}