import pickle
from argparse import ArgumentParser

import pandas as pd
from predict import predict, send_mail_absolute

parser = ArgumentParser()

parser.add_argument("--laptops_to_predict", type=str, help="preprocessed data for predict")
parser.add_argument("--trained_model", type=str, help="trained model pickle file")
parser.add_argument("--prediction_data", type=str, help="prediction out file")
args = parser.parse_args()


data = pd.read_csv(args.laptops_to_predict)
model = pickle.load(open(args.trained_model, "rb"))

print("Predicting price")
result = predict(data, model)

# save result
result.to_csv(args.prediction_data)
print(f"Data with predictions saved in: '{args.prediction_data}'")

# send email
send_mail_absolute(subject="Predictions", 
                   body=f"Predictions are saved in: {args.prediction_data}", 
                   to=["5vz9P@example.com"])
#
