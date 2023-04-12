from flask import Blueprint, render_template, request
import pickle

app_bp = Blueprint('main', __name__)

@app_bp.route('/')
def welcome():
    y = 'welcome to predict wildfire damage service'
    return y



@app_bp.route('/main/predict', methods =['GET', 'POST'])
def predict():
    import pandas as pd
    with open('model.pkl', 'rb') as f:
        model = pickle.load(f)

    # Get the form data
    tempavg = request.form.get('tempavg', type=float, default=0)
    humidrel = request.form.get('humidrel', type=float, default=0)
    windavg = request.form.get('windavg', type=float, default=0)
    raindays = request.form.get('raindays', type=float, default=0)

    # Make the prediction
    x = pd.DataFrame(columns=['tempavg', 'humidrel', 'windavg', 'raindays'])
    x.loc[0] = [tempavg, humidrel, windavg, raindays]
    prediction = model.predict(x.values)
    return render_template('predict.html', prediction=prediction)
