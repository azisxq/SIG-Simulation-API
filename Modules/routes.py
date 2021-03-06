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
	print('start')
	data_simulation = get_simulation_data(
		engine = engine,tabel_simulation = 'simulation_test', 
		simulation_status = "finish"
	)
	if len(data_simulation)==0:
		return jsonify({
			'status': 'load data error',
			'details': 'status ready to run not found' 
		}), 400

	print('progress_10%')

	data_cost = get_cost_data(tabel_cost='tabel_cost_segment_test')
	data_prediction_retail = get_cost_data(tabel_cost='tabel_rbp_retail_prediction_data_result_new')
	data_prediction_b2b = get_cost_data(tabel_cost='tabel_cbp_b2b_prediction_data_result_new')

	data_volume_retail = get_cost_data(tabel_cost='datamodellingpriceelasticityretail_new')
	data_volume_b2b = get_cost_data(tabel_cost='datamodellingpriceelasticityb2b_new')

	retail_distrik = get_cost_data(tabel_cost='tabel_prediction_retail_district_test_weighted')
	retail_distrik_ = retail_distrik[['period','province','district_ret','predict_med_new']]

	retail_province = get_cost_data(tabel_cost='tabel_prediction_retail_province_test_weighted')
	retail_province_ = retail_province[['period','province','predict_med_new']]


	print('progress_20%')

	kolom = data_simulation.columns
	print(kolom)
	print('data_simulation')
	print(len(data_simulation))
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

	print(data_simulation_b2b[['period', 'province','material type','district desc smi','ship to name', 'ship to code','productive plant']])

	data_simulation_retail = data_simulation[data_simulation['model']=='Retail']
	print('data_simulation_b2b')
	print(len(data_simulation_b2b))

	print('data_simulation_retail')
	print(len(data_simulation_retail))
	
	## Pre Processing B2B
	data_model_cbp_b2b = prep_cbp_modelling_b2b(data_simulation_b2b, data_prediction_b2b)
	print('data_model_cbp_b2b')
	print(len(data_model_cbp_b2b))
	
	print(data_model_cbp_b2b[['period', 'province','material type','district desc smi','ship to name', 'ship to code','productive plant']])

	data_model_volume_b2b = prep_vol_modelling_b2b(data_simulation_b2b, data_volume_b2b)
	print('data_model_volume_b2b')
	print(len(data_model_volume_b2b))

	print(data_model_volume_b2b[['period', 'province','material type','district desc smi','ship to name', 'ship to code','productive plant']])

	## Pre Processing Retail
	data_model_cbp_retail = prep_cbp_modelling_retail(data_simulation_retail, data_prediction_retail)
	print('data_model_cbp_retail')
	print(len(data_model_cbp_retail))
	print(data_model_cbp_retail.columns)

	data_model_volume_retail = prep_vol_modelling_retail(data_simulation_retail, data_volume_retail)
	print('data_model_volume_retail')
	print(len(data_model_volume_retail))


	print('progress_45%')

	## Apply model B2B
	if len(data_model_cbp_b2b) != 0:
		data_res_cbp_b2b = apply_model_b2b(data_model_cbp_b2b,'Price')
		print(len(data_res_cbp_b2b))

	if len(data_model_volume_b2b) !=0:
		data_res_volume_b2b = apply_model_b2b(data_model_volume_b2b,'Volume')
		print(len(data_res_volume_b2b))

	## Apply model Retail
	if len(data_model_cbp_retail) != 0:
		data_res_cbp_retail = apply_model_retail(data_model_cbp_retail,'Price')
		# print(len(data_res_cbp_retail))


	if len(data_model_volume_retail) !=0:
		data_res_volume_retail = apply_model_retail(data_model_volume_retail,'Volume')
		# print(len(data_res_volume_retail))


	# print(data_res_cbp_b2b.columns)
	# print(data_res_volume_b2b.columns)
	# print(data_res_cbp_retail.columns)
	# print(data_res_volume_retail.columns)


	# print(data_res_cbp_b2b[['period', 'province','material type','ship to name', 'ship to code','productive plant']])
	# print(data_res_volume_b2b[['period', 'province','material type','ship to name', 'ship to code','productive plant']])

	data_res = pd.DataFrame()
	if len(data_model_cbp_b2b) !=0:
		data_res = data_res.append(data_res_cbp_b2b[kolom], ignore_index=True)
	if len(data_model_volume_b2b) !=0:
		data_res = data_res.append(data_res_volume_b2b[kolom], ignore_index=True)
	if len(data_model_cbp_retail) !=0:
		data_res = data_res.append(data_res_cbp_retail[kolom], ignore_index=True)
	if len(data_model_volume_retail) !=0:
		data_res = data_res.append(data_res_volume_retail[kolom], ignore_index=True)

	print(len(data_res))
	print('progress_75%')
	print(data_res.columns)

	print(data_res[['period', 'province','material type','ship to name', 'ship to code','productive plant']])
	data_res_cost = calculate_cost(data_res, data_cost, retail_distrik_, retail_province_)
	data_res_cost = data_res_cost[kolom]
	print(len(data_res_cost))

	print('progress_85%')

	data_res_cek = cek_makesense(data_res_cost)
	data_res_cek = data_res_cek[kolom]
	print('progress_90%')

	try:
		status_update = update_simulation_data(
			engine=engine,data_simulation=data_res_cek, 
			status="finish", simulation_table='simulation_test_2_b2b_res'
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

	material_type = utils.validate(
		request.args, 'material_type',
		str, None
	)
	if material_type is None:
		return jsonify({
			'status': 'please specify material type input'
		}), 412
	if material_type.upper() not in config.b2b_new_cust['material_type']:
		return jsonify({
			'status': 'material type not in model',
			'details': 'you can only choose between this values {0!s}'.format(config.b2b_new_cust['material_type'])
		}), 412

	packaging_mode = utils.validate(
		request.args, 'packaging_mode',
		str, 0
	)
	if packaging_mode is None:
		return jsonify({
			'status': 'please specify packaging mode input'
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
		'material_type':material_type.lower(),
		'packaging_mode':packaging_mode.lower(),
		'term_of_payment':term_of_payment,
		'segmentsi': segment_si,
		'cluster': cluster
	},index=[0])

	print('load data from redshift')
	data_mapping = get_mapping_district_var(
		engine,
		tabel_mapping_var='mapping_district_var_customer_baru'
	)
	print('load data selesai')
	data_mapping['district'] = list(map(lambda x: x.lower(),data_mapping['district']))
	data_mapping['material_type'] = list(map(lambda x: x.lower(),data_mapping['material_type']))
	data_mapping['packaging_mode'] = list(map(lambda x: x.lower(),data_mapping['packaging_mode']))

	print(b2b_req['district'])
	print(b2b_req['material_type'])
	print(b2b_req['packaging_mode'])

	
	merge_mapping = pd.merge(
		b2b_req,
		data_mapping,
		left_on=['district','material_type','packaging_mode'],
		right_on=['district','material_type','packaging_mode'],
		how='left'
	)
	print(merge_mapping)

	if len(merge_mapping.dropna())==0:
		merge_mapping = pd.merge(
			b2b_req,
			data_mapping,
			left_on=['district','material_type'],
			right_on=['district','material_type'],
			how='left'
		)
	if len(merge_mapping.dropna())==0:
		if district_ref is None:
			return jsonify({
				'status': 'please specify district ref input because data is not found on district'
			}), 400
		else:
			merge_mapping = pd.merge(
				b2b_req,
				data_mapping,
				left_on=['district_ref','material_type','packaging_mode'],
				right_on=['district','material_type','packaging_mode'],
				how='left'
			)
			if len(merge_mapping.dropna())==0:
				merge_mapping = pd.merge(
					b2b_req,
					data_mapping,
					left_on=['district_ref','material_type'],
					right_on=['district','material_type'],
					how='left'
				)
			if len(merge_mapping.dropna())==0:
				return jsonify({
					'status': 'please check district ref, material type, and packaging type input because data with its combination is not found'
				}), 400
			else:
				pred = apply_model_cust_new_b2b(merge_mapping)
				last_cbp_distrik = merge_mapping['cbp_sig'][0]
				print(pred)
				return jsonify({
					'recommended cbp': pred[0],
					'last cbp distrik': last_cbp_distrik,
					'status': 'ok'
				}), 200

	if merge_mapping['nbc_brand'][0]=='UNKNOWN':
		if district_ref is None:
			return jsonify({
				'status': 'please specify district ref input because data is not found on district'
			}), 400
		else:
			b2b_req['last_cbp_sig'] = merge_mapping['cbp_sig']
			merge_mapping = pd.merge(
				b2b_req,
				data_mapping,
				left_on=['district_ref','material_type','packaging_mode'],
				right_on=['district','material_type','packaging_mode'],
				how='left'
			)
			if merge_mapping['nbc_brand'][0]=='UNKNOWN':
				merge_mapping = pd.merge(
					b2b_req,
					data_mapping,
					left_on=['district_ref','material_type'],
					right_on=['district','material_type'],
					how='left'
				)
			if merge_mapping['nbc_brand'][0]=='UNKNOWN':
				return jsonify({
					'status': 'please check district ref, material type, and packaging type input because data with its combination is not found'
				}), 400
			else:
				pred = apply_model_cust_new_b2b(merge_mapping)
				last_cbp_distrik = b2b_req['last_cbp_sig'][0]
				print(pred)
				return jsonify({
					'recommended cbp': pred[0],
					'last cbp distrik': last_cbp_distrik,
					'status': 'ok'
				}), 200
	else:
		pred = apply_model_cust_new_b2b(merge_mapping)
		last_cbp_distrik = merge_mapping['cbp_sig'][0]
		print(pred)
		return jsonify({
			'recommended cbp': pred[0],
			'last cbp distrik': last_cbp_distrik,
			'status': 'ok'
		}), 200







