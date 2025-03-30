import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


def train_model(data: pd.DataFrame, test_size: float) -> RandomForestRegressor:
    X, y = data.drop("Price_euros", axis=1), data["Price_euros"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    forest = RandomForestRegressor()
    forest.fit(X_train_scaled, y_train)
    test_score = forest.score(X_test_scaled, y_test)
    print(f"\tTest score: {test_score}")
    return forest
