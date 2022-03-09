from flask import Flask
from flask import jsonify
from flask import request
import pandas as pd
from Modules import config
from Modules.methods import *
from Modules.common import utils
import time

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