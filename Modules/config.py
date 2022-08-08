b2b_new_cust = {
	"packaging_mode" :['bulk','jumbo bag', 'bag'],
	"segmentsi" : ['cpm & kontraktor', 'beton rmx', 'bumn'],
	"cluster":['bronze', 'platinum', 'silver', 'gold']	
}

model_elasticity_retail = {
	"ST" : "model_3_nasional_st.pkl",
	"SG" : "model_3_nasional_sg.pkl",
	"DYNAMIX":"model_3_nasional_dynamix.pkl",
	"POWERMAX": "model_3_nasional_powermax.pkl",
	"SP":"model_3_nasional_sp.pkl",
	"ANDALAS":"model_3_nasional_andalas.pkl",
	"MASONRY":"model_3_nasional_masonry.pkl"
}

model_pricing_retail = {
	"ST" : "model_5_nasional_st_price (1).pkl",
	"SG" : "model_5_nasional_sg_price (1).pkl",
	"DYNAMIX":"model_5_nasional_dynamix_price (1).pkl",
	"POWERMAX": "model_5_nasional_powermax_price (1).pkl",
	"SP":"model_5_nasional_sp_price (1).pkl",
	"ANDALAS":"model_5_nasional_andalas_price (1).pkl",
	"MASONRY":"model_5_nasional_masonry_price (1).pkl"
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

path_data_mapping_bisnis_retail = './Modules/data/mapping_kategori_logika_bisnis.csv'
path_data_mapping_bisnis_b2b = './Modules/data/mapping_kategori_bisnis_b2b.csv'
path_data_mapping_bisnis_b2b_sow_100 = './Modules/data/mapping_kategori_bisnis_b2b_sow_100.csv'