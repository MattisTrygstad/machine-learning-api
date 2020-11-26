from fetcher import Fetcher
from flask import Response
from flask import jsonify
import numpy as np

import googleapiclient.discovery
from google.api_core.client_options import ClientOptions
import datetime
import json


def get_data(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)

    # Set CORS headers for the main request
    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    if 'date' in request.args:
        date_format = '%Y-%m-%d'
        prediction_date = datetime.datetime.strptime(request.args['date'], date_format)
        previous_date = prediction_date - datetime.timedelta(1)

        df_prediction = Fetcher().fetch_data(previous_date.strftime(date_format), previous_date.strftime(date_format))
        instances = df_prediction.iloc[:, 1:].values.tolist()
        response = predict_json('tdt4173-292607', 'europe-west4', 'downfall_prediction_model', instances, 'downfall_prediction_model_v1')

        if (prediction_date.date() != datetime.datetime.today().date()):
            df_actual = Fetcher().fetch_data(prediction_date.strftime(date_format), prediction_date.strftime(date_format))
            actual_value = df_actual.iloc[-1, -1]
            response_dict = {'predicted_value': round(response[0][0], 1), 'actual_value': round(actual_value, 1)}
        else:
            response_dict = {'predicted_value': round(response[0][0], 1), 'actual_value': None}

        return (jsonify(response_dict), 200, headers)

    else:
        return request.args


def predict_json(project, region, model, instances, version=None):
    """Send json data to a deployed model for prediction.

   Args:
        project(str): project where the Cloud ML Engine Model is deployed.
        region(str): regional endpoint to use
        set to None for ml.googleapis.com
        model(str): model name.
        instances([Mapping[str: Any]]): Keys should be the names of Tensors
           your deployed model expects as inputs. Values should be datatypes
            convertible to Tensors, or (potentially nested) lists of datatypes
            convertible to tensors.
        version: str, version of the model to target.
    Returns:
        Mapping[str: any]: dictionary of prediction results defined by the
           model.
    """
    # Create the ML Engine service object.
    # To authenticate set the environment variable
    # GOOGLE_APPLICATION_CREDENTIALS=<path_to_service_account_file>
    prefix = "{}-ml".format(region) if region else "ml"
    api_endpoint = "https://{}.googleapis.com".format(prefix)
    client_options = ClientOptions(api_endpoint=api_endpoint)
    service = googleapiclient.discovery.build(
        'ml', 'v1', client_options=client_options)
    name = 'projects/{}/models/{}'.format(project, model)

    if version is not None:
        name += '/versions/{}'.format(version)

    response = service.projects().predict(
        name=name,
        body={'instances': [instances]}
    ).execute()

    if 'error' in response:
        raise RuntimeError(response['error'])

    return response['predictions']
