from pathlib import Path

import pandas as pd

from training.knn_regressor_hyperparams_search import prep_data, prep_scaler
from sklearn.preprocessing import StandardScaler

project_root = Path(__file__).resolve().parents[1]
data_path = project_root / "ingestion/ingestion_run/output/bike_rental_hourly_data_2022_2025.csv"

def test_prep_data():

    df = pd.read_csv(data_path, nrows=100) # use 100 rows for faster testing

    x_train, y_train, x_test, y_test = prep_data.fn(df)

    expected_feature_columns = ["Month", "Day", "Hour", "temperature"]

    assert len(expected_feature_columns) == len(x_train.columns)
    assert len(expected_feature_columns) == len(x_test.columns)

    assert len(y_train.columns) == 1
    assert len(y_test.columns) == 1

    for col in expected_feature_columns:
        assert col in x_train.columns
        assert col in x_train.columns

    assert "total_classic_bike_rental" in y_train.columns
    assert "total_classic_bike_rental" in y_test.columns




def case_prep_scaler(x_df,y_df,selected_feature_columns,target_should_scale):

    # test prep_scaler
    feature_scalers, target_scaler = prep_scaler(x_df,y_df, selected_feature_columns, scale_target = target_should_scale)

    # prep verification answer
    expected_feature_columns = sorted(x_df.columns)

    expected_feature_idx_map = {}
    cnt = 0
    for col in expected_feature_columns:
        expected_feature_idx_map[col] = cnt
        cnt = cnt + 1

    # verification
    for name,scaler,idx in feature_scalers:

        assert isinstance(scaler, StandardScaler) 
        assert expected_feature_idx_map[name] == idx

    if(target_should_scale):
        assert isinstance(target_scaler , StandardScaler) 
    else:
        assert target_scaler == None 





def test_prep_scaler():

    # prep

    df = pd.read_csv(data_path, nrows=100)
    # use 100 rows for fatser testing, the resulting mean and std used by scalers do not matter

    df["total_classic_bike_rental"] = df['classic_bike_casual_count'] + df['classic_bike_member_count']

    feature_columns = ["Month", "Day", "Hour", "temperature"]
    target_column = ["total_classic_bike_rental"]

    x_df = df[feature_columns] # include all year anyway
    y_df = df[target_column] 

    # end prep

    # case 1
    print("test prep scaler case 1")
    selected_feature_columns = ["Month", "Day", "Hour", "temperature"]
    case_prep_scaler(x_df,y_df,selected_feature_columns,True)

    print("test prep scaler case 2")
    selected_feature_columns = ["Month", "temperature"]
    case_prep_scaler(x_df,y_df,selected_feature_columns,False)

    print("test prep scaler case 3")
    selected_feature_columns = ["temperature"]
    case_prep_scaler(x_df,y_df,selected_feature_columns,True)

    print("test prep scaler case 4")
    selected_feature_columns = []
    case_prep_scaler(x_df,y_df,selected_feature_columns,False)


