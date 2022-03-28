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