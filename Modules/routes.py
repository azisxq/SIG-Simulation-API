from flask import Flask
from flask import jsonify
from flask import request
import pandas as pd
from Modules import config
from Modules.methods import *
from Modules.common import utils
import time
import math
from Modules.predict_new_cust_b2b import *

app = Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def root():
	return jsonify({'status': 'ok'}), 200


@app.route('/calculate_simulation', methods=['GET'])
def predict():
	data_simulation = get_simulation_data(
		engine = engine,tabel_simulation = 'simulation_test', 
		simulation_status = "ready to run"
	)
	if len(data_simulation)==0:
		return jsonify({
			'status': 'load data error',
			'details': 'status ready to run not found' 
		}), 400

	print('progress_10%')

	data_cost = get_cost_data(tabel_cost='tabel_cost_segment_test')
	data_prediction_retail = get_cost_data(tabel_cost='retail_rbp_prediction_test_202201')
	data_prediction_b2b = get_cost_data(tabel_cost='cbp_prediction_test_202201')
	print('progress_20%')

	kolom = data_simulation.columns
	# try:
	# 	status_update = update_simulation_data(
	# 		engine=engine,data_simulation=data_simulation, 
	# 		status="running", simulation_table='simulation_test'
	# 	)
	# except Exception as e:
	# 	print(e)
	# 	return jsonify({
	# 		'status': '{0!s}'.format(e)
	# 	}), 400
	print('progress_30%')

	data_simulation_b2b = data_simulation[data_simulation['model']=='B2B']
	data_simulation_retail = data_simulation[data_simulation['model']=='Retail']
	
	## Pre Processing B2B
	data_model_cbp_b2b = prep_cbp_modelling_b2b(data_simulation_b2b, data_prediction_b2b)
	data_model_volume_b2b = prep_vol_modelling_b2b(data_simulation_b2b)

	## Pre Processing Retail
	data_model_cbp_retail = prep_cbp_modelling_retail(data_simulation_retail, data_prediction_retail)
	data_model_volume_retail = prep_vol_modelling_b2b(data_simulation_retail)
	print('progress_45%')

	## Apply model B2B
	if len(data_model_cbp_b2b) != 0:
		data_res_cbp_b2b = apply_model_b2b(data_model_cbp_b2b,'Price')
	if len(data_model_volume_b2b) !=0:
		data_res_volume_b2b = apply_model_b2b(data_model_volume_b2b,'Volume')

	## Apply model Retail
	if len(data_model_cbp_retail) != 0:
		data_res_cbp_retail = apply_model_retail(data_model_cbp_retail,'Price')
	if len(data_model_volume_retail) !=0:
		data_res_volume_retail = apply_model_retail(data_model_volume_retail,'Volume')

	data_res = pd.DataFrame(columns = kolom)
	if len(data_model_cbp_b2b) !=0:
		data_res = data_res.append(data_res_cbp_b2b[kolom], ignore_index=True)
	if len(data_model_volume_b2b) !=0:
		data_res = data_res.append(data_res_volume_b2b[kolom], ignore_index=True)
	if len(data_model_cbp_retail) !=0:
		data_res = data_res.append(data_res_cbp_retail[kolom], ignore_index=True)
	if len(data_model_volume_retail) !=0:
		data_res = data_res.append(data_res_volume_retail[kolom], ignore_index=True)
	print('progress_75%')

	data_res_cost = calculate_cost(data_res, data_cost)
	print('progress_90%')

	try:
		status_update = update_simulation_data(
			engine=engine,data_simulation=data_res_cost, 
			status="finish", simulation_table='simulation_test'
		)
	except Exception as e:
		return jsonify({
			'status': 'save update error',
			'details': '{0!s}'.format(e) 
		}), 400
	print('progress_100%')
	return jsonify({
		'status': 'ok'
	}), 200


@app.route('/cbp_new_cust', methods=['GET'])
def b2b_new_cust():
	province = utils.validate(
		request.args, 'province',
		str, None
	)
	if province is None:
		return jsonify({
			'status': 'please specify province input'
		}), 412

	district = utils.validate(
		request.args, 'district',
		str, None
	)
	if district is None:
		return jsonify({
			'status': 'please specify district input'
		}), 412

	district_ref = utils.validate(
		request.args, 'district_ref',
		str, None
	)

	demand = utils.validate(
		request.args, 'demand',
		float, None
	)
	if demand is None:
		return jsonify({
			'status': 'please specify demand input'
		}), 412

	product_type = utils.validate(
		request.args, 'product_type',
		str, None
	)
	if product_type is None:
		return jsonify({
			'status': 'please specify product type input'
		}), 412
	if product_type.lower() not in config.b2b_new_cust['product_type']:
		return jsonify({
			'status': 'product type not in model',
			'details': 'you can only choose between this values {0!s}'.format(config.b2b_new_cust['product_type'])
		}), 412

	packaging_mode = utils.validate(
		request.args, 'packaging_mode',
		str, 0
	)
	if product_type is None:
		return jsonify({
			'status': 'please specify product type input'
		}), 412
	if packaging_mode.lower() not in config.b2b_new_cust['packaging_mode']:
		return jsonify({
			'status': 'packaging mode not in model',
			'details': 'you can only choose between this values {0!s}'.format(config.b2b_new_cust['packaging_mode'])
		}), 412

	term_of_payment = utils.validate(
		request.args, 'term_of_payment',
		float, None
	)
	if term_of_payment is None:
		return jsonify({
			'status': 'please specify term of payment input'
		}), 412

	segment_si = utils.validate(
		request.args, 'segment_si',
		str, None
	)
	if segment_si is None:
		return jsonify({
			'status': 'please specify segment si input'
		}), 412
	if segment_si.lower() not in config.b2b_new_cust['segmentsi']:
		return jsonify({
			'status': 'segment si not in model',
			'details': 'you can only choose between this values {0!s}'.format(config.b2b_new_cust['segmentsi'])
		}), 412

	cluster = utils.validate(
		request.args, 'cluster',
		str, None
	)
	if cluster is None:
		return jsonify({
			'status': 'please specify cluster input'
		}), 412
	if cluster.lower() not in config.b2b_new_cust['cluster']:
		return jsonify({
			'status': 'cluster not in model',
			'details': 'you can only choose between this values {0!s}'.format(config.b2b_new_cust['cluster'])
		}), 412

	b2b_req = pd.DataFrame({
		'province':province.lower(),
		'is_jawa_bali':grouping_province(province),
		'district':district.lower(),
		'district_ref':district_ref.lower(),
		'demand':demand,
		'product_type':product_type.lower(),
		'product_type_kfold_target_enc':transform_encoded_new_cust('product_type',product_type.lower()),
		'packaging_mode':packaging_mode.lower(),
		'packaging_mode_kfold_target_enc':transform_encoded_new_cust('packaging_mode',packaging_mode.lower()),
		'term_of_payment':term_of_payment,
		'segmentsi': segment_si,
		'segmentsi_kfold_target_enc':transform_encoded_new_cust('segmentsi',segment_si.lower()),
		'cluster': cluster,
		'cluster_kfold_target_enc':transform_encoded_new_cust('cluster',cluster.lower())
	},index=[0])

	print('load data from redshift')
	data_mapping = get_mapping_district_var(
		engine,
		tabel_mapping_var='mapping_district_var_customer_baru_test'
	)
	print('load data selesai')
	data_mapping['district'] = list(map(lambda x: x.lower(),data_mapping['district']))
	data_mapping['product_type'] = list(map(lambda x: x.lower(),data_mapping['product_type']))
	data_mapping['packaging_mode'] = list(map(lambda x: x.lower(),data_mapping['packaging_mode']))

	merge_mapping = pd.merge(
		b2b_req,
		data_mapping,
		left_on=['district','product_type','packaging_mode'],
		right_on=['district','product_type','packaging_mode'],
		how='left'
	)
	print(merge_mapping)
	if len(merge_mapping.dropna())==0:
		if district_ref is None:
			return jsonify({
				'status': 'please specify district ref input because data is not found on district'
			}), 400
		else:
			merge_mapping = pd.merge(
				b2b_req,
				data_mapping,
				left_on=['district_ref','product_type','packaging_mode'],
				right_on=['district','product_type','packaging_mode'],
				how='left'
			)
			if len(merge_mapping.dropna())==0:
				return jsonify({
					'status': 'please check district ref, product type, and packaging type input because data with its combination is not found'
				}), 400
			else:
				pred = apply_model_cust_new_b2b(merge_mapping)
				print(pred)
				return jsonify({
					'recommended cbp': pred[0],
					'status': 'ok'
				}), 200
	else:
		pred = apply_model_cust_new_b2b(merge_mapping)
		print(pred)
		return jsonify({
			'recommended cbp': pred[0],
			'status': 'ok'
		}), 200







