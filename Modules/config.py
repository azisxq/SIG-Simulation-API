b2b_new_cust = {
	"material_type" : ['OPC PREMIUM', 'OPC REGULER','OWC','PCC PREMIUM', 'PCC', 'PPC', 'SBC','TYPE II', 'TYPE V'],
	"packaging_mode" :['bulk','jumbo bag'],
	"segmentsi" : ['cpm & kontraktor', 'beton rmx', 'bumn'],
	"cluster":['bronze', 'platinum', 'silver', 'gold']	
}

model_elasticity_retail = {
    "ST" : "STDTree-Copy1.pkl",
    "SG" : "SGDTree-Copy1.pkl",
    "DYNAMIX":"DYNAMIXDTree-Copy1.pkl",
    "POWERMAX":"POWERMAXDTree-Copy1.pkl",
    "SP":"SPDTree-Copy1.pkl",
    "ANDALAS":"ANDALASDTree-Copy1.pkl",
    "MASONRY":"MASONRYDTree-Copy1.pkl"
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