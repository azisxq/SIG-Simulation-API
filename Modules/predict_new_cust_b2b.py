from Modules.methods import *
from Modules.config import *

def apply_model_cust_new_b2b(data):
	var_x = [
		'segmentsi_kfold_target_enc',
		'cluster_kfold_target_enc',
		'term_of_payment',
		'plant_to_distance_sig_nbc',
		'packaging_mode_kfold_target_enc',
		'cbp_nbc',
		'product_type_kfold_target_enc',
		'is_jawa_bali',
		'cbp_sig',
		'plant_to_distance_sig'
	]
	file = open("./Modules/data/predict_b2b_new_cust_test.pkl",'rb')
	cbp_model = pickle.load(file)
	file.close()
	prediction_cbp = cbp_model.predict(data[var_x])
	return prediction_cbp