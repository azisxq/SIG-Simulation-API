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
		simulation_status = "ready to run"
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
	
	print('progress_30%')

	data_simulation_b2b = data_simulation[data_simulation['model']=='B2B']

	data_simulation_retail = data_simulation[data_simulation['model']=='Retail']
	
	## Pre Processing B2B
	data_model_cbp_b2b = prep_cbp_modelling_b2b(data_simulation_b2b, data_prediction_b2b)
	data_model_volume_b2b = prep_vol_modelling_b2b(data_simulation_b2b, data_volume_b2b)

	## Pre Processing Retail
	data_model_cbp_retail = prep_cbp_modelling_retail(data_simulation_retail, data_prediction_retail)
	
	data_model_volume_retail = prep_vol_modelling_retail(data_simulation_retail, data_volume_retail)
	

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

	data_res = pd.DataFrame()
	if len(data_model_cbp_b2b) !=0:
		data_res = data_res.append(data_res_cbp_b2b[kolom], ignore_index=True)
	if len(data_model_volume_b2b) !=0:
		data_res = data_res.append(data_res_volume_b2b[kolom], ignore_index=True)
	if len(data_model_cbp_retail) !=0:
		data_res = data_res.append(data_res_cbp_retail[kolom], ignore_index=True)
	if len(data_model_volume_retail) !=0:
		data_res = data_res.append(data_res_volume_retail[kolom], ignore_index=True)

	print('progress_75%')

	data_res_cost = calculate_cost(data_res, data_cost, retail_distrik_, retail_province_)
	data_res_cost = data_res_cost[kolom]

	print('progress_85%')

	data_res_cek = cek_makesense(data_res_cost)
	data_res_cek = data_res_cek[kolom]
	print('progress_90%')

	try:
		status_update = update_simulation_data(
			engine=engine,data_simulation=data_res_cek, 
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
	if district_ref is None:
		return jsonify({
			'status': 'please specify district reference input'
		}), 412

	demand = utils.validate(
		request.args, 'demand',
		float, None
	)
	if demand is None:
		return jsonify({
			'status': 'please specify demand input'
		}), 412

	group_pelanggan = utils.validate(
		request.args, 'group_pelanggan',
		str, None
	)
	if group_pelanggan is None:
		return jsonify({
			'status': 'please specify group_pelanggan input'
		}), 412

	material_type = utils.validate(
		request.args, 'material_type',
		str, None
	)
	if material_type is None:
		return jsonify({
			'status': 'please specify material type input'
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

	cbp_nbc = utils.validate(
		request.args, 'cbp_nbc',
		float, 0
	)

	nbc_brand = utils.validate(
		request.args, 'nbc_brand',
		str, 'UNKNOWN'
	)

	b2b_req = pd.DataFrame({
		'province':province.lower(),
		'district':district.lower(),
		'district_ref':district_ref.lower(),
		'demand':demand,
		'group_pelanggan':group_pelanggan.lower(),
		'material_type':material_type.lower(),
		'packaging_mode':packaging_mode.lower(),
		'term_of_payment':term_of_payment,
		'segmentsi': segment_si.lower(),
		'cluster': cluster.lower(),
		'nbc_brand': nbc_brand,
		'cbp_nbc': cbp_nbc
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

	if nbc_brand != "UNKNOWN":
		merge_mapping = pd.merge(
			b2b_req,
			data_mapping,
			left_on=['district','material_type','packaging_mode','nbc_brand'],
			right_on=['district','material_type','packaging_mode','nbc_brand'],
			how='left'
		)
		if len(merge_mapping.dropna())==0:
			print(b2b_req[['district','material_type','nbc_brand']])
			merge_mapping = pd.merge(
				b2b_req,
				data_mapping,
				left_on=['district','material_type','nbc_brand'],
				right_on=['district','material_type','nbc_brand'],
				how='left'
			)
			if len(merge_mapping.dropna())==0:
				merge_mapping = pd.merge(
					b2b_req,
					data_mapping,
					left_on=['district','material_type'],
					right_on=['district','material_type'],
					how='left'
				)
				merge_mapping = merge_mapping[:1]
				print(merge_mapping)
				last_cbp_distrik = merge_mapping['cbp_sig'][0]
				distance_to_plant_sig = merge_mapping['plant_to_distance_sig'][0]
			else:
				last_cbp_distrik = merge_mapping['cbp_sig'][0]
				distance_to_plant_sig = merge_mapping['plant_to_distance_sig'][0]
			merge_mapping = pd.merge(
				b2b_req,
				data_mapping,
				left_on=['district_ref','material_type','nbc_brand'],
				right_on=['district','material_type','nbc_brand'],
				how='left'
			)
			print(last_cbp_distrik)
			print(distance_to_plant_sig)
			merge_mapping = merge_mapping[:1]
			merge_mapping['cbp_sig'] = last_cbp_distrik
			merge_mapping['plant_to_distance_sig'] =  distance_to_plant_sig

			print(merge_mapping)
			pred = apply_model_cust_new_b2b(merge_mapping)
			return jsonify({
				'recommended cbp': pred[0],
				'last cbp distrik': last_cbp_distrik,
				'status': 'ok'
			}), 200
		else:
			merge_mapping = merge_mapping[:1]
			print(2)
			pred = apply_model_cust_new_b2b(merge_mapping)
			last_cbp_distrik = merge_mapping['cbp_sig'][0]
			print(pred)
			return jsonify({
				'recommended cbp': pred[0],
				'last cbp distrik': last_cbp_distrik,
				'status': 'ok'
			}), 200
	elif nbc_brand == "UNKNOWN":
		merge_mapping = pd.merge(
			b2b_req,
			data_mapping,
			left_on=['district','material_type','packaging_mode'],
			right_on=['district','material_type','packaging_mode'],
			how='left'
		)
		if len(merge_mapping.dropna())==0:
			merge_mapping = pd.merge(
				b2b_req,
				data_mapping,
				left_on=['district','material_type'],
				right_on=['district','material_type'],
				how='left'
			)
			if len(merge_mapping.dropna())==0:
				merge_mapping = pd.merge(
					b2b_req,
					data_mapping,
					left_on=['district'],
					right_on=['district'],
					how='left'
				)
				if len(merge_mapping.dropna())==0:
					return jsonify({
						'status': 'please check district ref and material type input because data with that combination is not found'
					}), 400
				merge_mapping = merge_mapping[:1]
				print(3)
				pred = apply_model_cust_new_b2b(merge_mapping)
				last_cbp_distrik = 0
				print(pred)
				return jsonify({
					'recommended cbp': pred[0],
					'last cbp distrik': last_cbp_distrik,
					'status': 'ok'
				}), 200
			merge_mapping = merge_mapping[:1]
			print(4)
			pred = apply_model_cust_new_b2b(merge_mapping)
			last_cbp_distrik = merge_mapping['cbp_sig'][0]
			print(pred)
			return jsonify({
				'recommended cbp': pred[0],
				'last cbp distrik': last_cbp_distrik,
				'status': 'ok'
			}), 200
		merge_mapping = merge_mapping[:1]
		print(5)
		pred = apply_model_cust_new_b2b(merge_mapping)
		last_cbp_distrik = merge_mapping['cbp_sig'][0]
		print(pred)
		return jsonify({
			'recommended cbp': pred[0],
			'last cbp distrik': last_cbp_distrik,
			'status': 'ok'
		}), 200







