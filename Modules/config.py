b2b_new_cust = {
	"material_type" : ['OPC PREMIUM', 'OPC REGULER','OWC','PCC PREMIUM', 'PCC', 'PPC', 'SBC','TYPE II', 'TYPE V'],
	"packaging_mode" :['bulk','jumbo bag'],
	"segmentsi" : ['cpm & kontraktor', 'beton rmx', 'bumn'],
	"cluster":['bronze', 'platinum', 'silver', 'gold']	
}

model_elasticity_retail = {
    "ST" : "GBoost_st_elasticity_brand_sig_model_price.pkl",
    "SG" : "GBoost_sg_elasticity_brand_sig_model_price.pkl",
    "DYNAMIX":"DTree_dynamix_elasticity_brand_sig_model_price.pkl",
    "POWERMAX": "GBoost_powermax_elasticity_brand_sig_model_price.pkl",
    "SP":"GBoost_sp_elasticity_brand_sig_model_price.pkl",
    "ANDALAS":"GBoost_andalas_elasticity_brand_sig_model_price.pkl",
    "MASONRY":"RForest_masonry_elasticity_brand_sig_model_price.pkl"
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