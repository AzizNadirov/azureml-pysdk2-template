import pickle
from argparse import ArgumentParser

import pandas as pd
from train import train_model

parser = ArgumentParser()

parser.add_argument("--preprocessed_laptops_data", type=str, help="preprocessed laptops data path")
parser.add_argument("--test_size", type=str, help="test_size", default="0.15")
parser.add_argument("--trained_model", type=str, help="test_size")
args = parser.parse_args()


data = pd.read_csv(args.preprocessed_laptops_data)
test_size = float(args.test_size)
print(f"Train model on data size: {data.shape[0]}")
print(f"\tTest size: {test_size}")
model = train_model(data, test_size=test_size)
print("\tDone")

# save model
model_save_as = args.trained_model
pickle.dump(model, open(model_save_as, "wb"))
print(f"Model saved as '{model_save_as}'")
#
