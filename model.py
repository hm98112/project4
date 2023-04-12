from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
import pandas as pd
import psycopg2
from sklearn.metrics import mean_squared_error, mean_absolute_error
import pickle



conn =  psycopg2.connect(
    host = "localhost",
    database = "section4_PR",
    user = "postgres",
    password = ""
)

df = pd.read_sql_query("SELECT * FROM fire_detail2", conn)

conn.close()

target = 'dmgarea'

X_train, X_test, y_train, y_test = train_test_split(df.drop(columns= [target, 'ocurdt', 'ocurdo']), df[target], test_size = 0.2, random_state= 42)

print(X_train.columns)
model = XGBRegressor(
    objective="reg:squarederror",
    eval_metric="rmse", 
    n_estimators=200,
    random_state=42,
    n_jobs=-1,
    max_depth=7,
    learning_rate=0.1,
    )



model.fit(X_train, y_train)
y_pred = model.predict(X_test)

mse = mean_squared_error(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)

print('Mean Squared Error:', mse)
print('Mean Absolute Error:', mae)





with open('model.pkl', 'wb') as f:
    pickle.dump(model, f)
    