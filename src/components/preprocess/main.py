from argparse import ArgumentParser

import pandas as pd
from prep import preprocess

parser = ArgumentParser()

parser.add_argument("--laptop_price_data", type=str, help="laptops data path")
parser.add_argument("--preprocessed_laptops_data", type=str, help="preprocessed data output")
args = parser.parse_args()

laptops_data = pd.read_csv(args.laptop_price_data, encoding="latin-1")
print("Preprocess data:")
prep_data = preprocess(laptops_data)
print("\tDone")

# save
prep_data.to_csv(args.preprocessed_laptops_data, index=False)
#
