import pandas as pd
from loguru import logger

def turn_memory_into_MB(value):
    if "GB" in value:
        return float(value[: value.find("GB")]) * 1000
    elif "TB" in value:
        return float(value[: value.find("TB")]) * 1000000


def preprocess(laptops: pd.DataFrame) -> pd.DataFrame:
    """preprocess passed 'laptop_price_data' data"""
    laptops = laptops.copy()
    laptops = laptops.drop("Product", axis=1)
    laptops = laptops.join(pd.get_dummies(laptops.Company))
    laptops = laptops.drop("Company", axis=1)
    laptops = laptops.join(pd.get_dummies(laptops.TypeName))
    laptops = laptops.drop("TypeName", axis=1)
    laptops["ScreenResolution"] = laptops.ScreenResolution.str.split(" ").apply(lambda x: x[-1])
    laptops["Screen Width"] = laptops.ScreenResolution.str.split("x").apply(lambda x: x[0])
    laptops["Screen Height"] = laptops.ScreenResolution.str.split("x").apply(lambda x: x[1])
    laptops = laptops.drop("ScreenResolution", axis=1)
    laptops["CPU Brand"] = laptops.Cpu.str.split(" ").apply(lambda x: x[0])
    laptops["CPU Frequency"] = laptops.Cpu.str.split(" ").apply(lambda x: x[-1])
    laptops = laptops.drop("Cpu", axis=1)
    laptops["CPU Frequency"] = laptops["CPU Frequency"].str[:-3]
    laptops["Ram"] = laptops["Ram"].str[:-2]
    laptops["Ram"] = laptops["Ram"].astype("int")
    laptops["CPU Frequency"] = laptops["CPU Frequency"].astype("float")
    laptops["Screen Width"] = laptops["Screen Width"].astype("int")
    laptops["Screen Height"] = laptops["Screen Height"].astype("int")
    laptops["Memory Amount"] = laptops.Memory.str.split(" ").apply(lambda x: x[0])
    laptops["Memory Type"] = laptops.Memory.str.split(" ").apply(lambda x: x[1])
    laptops["Memory Amount"] = laptops["Memory Amount"].apply(turn_memory_into_MB)
    laptops = laptops.drop("Memory", axis=1)
    laptops["Weight"] = laptops["Weight"].str[:-2]
    laptops["Weight"] = laptops["Weight"].astype("float")
    laptops["GPU Brand"] = laptops.Gpu.str.split(" ").apply(lambda x: x[0])
    laptops = laptops.drop("Gpu", axis=1)
    laptops = laptops.join(pd.get_dummies(laptops.OpSys))
    laptops = laptops.drop("OpSys", axis=1)
    cpu_categories = pd.get_dummies(laptops["CPU Brand"])
    cpu_categories.columns = [col + "_CPU" for col in cpu_categories.columns]
    laptops = laptops.join(cpu_categories)
    laptops = laptops.drop("CPU Brand", axis=1)
    gpu_categories = pd.get_dummies(laptops["GPU Brand"])
    gpu_categories.columns = [col + "_GPU" for col in gpu_categories.columns]
    laptops = laptops.join(gpu_categories)
    laptops = laptops.drop("GPU Brand", axis=1)
    target_correlations = laptops.corr(numeric_only=True)["Price_euros"].apply(abs).sort_values()
    logger.info(f"target_correlations: {target_correlations}")
    selected_features = target_correlations[-21:].index
    selected_features = list(selected_features)
    logger.info(f"selected_features: {selected_features}")
    preprocessed_df = laptops[selected_features]
    return preprocessed_df


if __name__ == "__main__":
    from IPython.display import display

    laptops = "/home/anadirov/Documents/Code/mp-ds-aml-base/local/laptop_price.csv"
    df = pd.read_csv(laptops, encoding="latin-1")
    # res = preprocess(df)
    display(df.head(5))
