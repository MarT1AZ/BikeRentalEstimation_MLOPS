from sklearn.neighbors import KNeighborsRegressor
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction import DictVectorizer
import mlflow

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error

import pandas as pd
import numpy as np
import os
import argparse
from pathlib import Path
import yaml
from prefect import task, flow
from hyperopt import STATUS_OK, Trials, fmin, hp, space_eval, tpe


mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URL"))
mlflow.set_experiment("bikerental-knn-regression-model")


def get_dvc_output_hash(dvc_lock_path, output_path):
    """INPUT: DVC lockfile path and tracked output path.

    RETURN: The DVC md5 hash recorded for the output.
    """
    with open(dvc_lock_path, "r", encoding="utf-8") as lock_file:
        lock_data = yaml.safe_load(lock_file)

    lock_root = Path(dvc_lock_path).resolve().parent
    requested_output = Path(output_path).resolve()
    for output in lock_data["stages"]["ingest_bike_rentals"]["outs"]:
        locked_output = (lock_root / output["path"]).resolve()
        if locked_output == requested_output:
            return output["md5"]

    raise ValueError(f"DVC output not found in lockfile: {output_path}")


@task
def load_data(data_path):
    """INPUT: Path to the aggregated bike-rental CSV file.

    RETURN: Raw bike-rental DataFrame loaded from ``data_path``.
    """
    df = pd.read_csv(data_path)
    return df


@task
def prep_data_and_scaler(df):
    """INPUT: Raw bike-rental DataFrame.

    RETURN: Chronologically split train/test feature records and target frames,
    plus scalers fitted on the training period only.
    """
    prepared_df = df.copy()

    # Agent Task
    # - Feature : Month,Day,Hour,temperature
    # - target : total_classic_bike_rental = classic_bike_casual_count + classic_bike_member_count

    # Sort by date time, but do not include the helper column in the features.
    prepared_df["datetime"] = pd.to_datetime(
        prepared_df[["Year", "Month", "Day", "Hour"]]
    )
    prepared_df = prepared_df.sort_values("datetime").reset_index(drop=True)

    prepared_df["total_classic_bike_rental"] = (
        prepared_df["classic_bike_casual_count"]
        + prepared_df["classic_bike_member_count"]
    )

    # Keep both feature and target frames raw here: the model packages each
    # scaler and applies the transforms exactly once during fit/predict.
    feature_columns = ["Month", "Day", "Hour", "temperature"]
    train_mask = prepared_df["Year"] != 2025
    train_df = prepared_df.loc[train_mask].copy()
    test_df = prepared_df.loc[~train_mask].copy()

    X_train_df = train_df[feature_columns].copy()
    X_test_df = test_df[feature_columns].copy()

    # turn to dict as model pipeline want dict vectorizer
    X_train_records = X_train_df.to_dict(orient="records")
    X_test_records = X_test_df.to_dict(orient="records")

    y_train_df = train_df[["total_classic_bike_rental"]].copy()
    y_test_df = test_df[["total_classic_bike_rental"]].copy()

    temp_scaler = StandardScaler()
    rental_scaler = StandardScaler()

    # Fit the temperature scaler on training observations before passing it to the pipeline.
    temp_scaler.fit(X_train_df[["temperature"]])

    # Fit the rental scaler on training observations before packaging it.
    rental_scaler.fit(y_train_df[["total_classic_bike_rental"]])

    # End task

    return (
        X_train_records,
        y_train_df,
        X_test_records,
        y_test_df,
        temp_scaler,
        rental_scaler,
    )


def build_knn_model(params, temp_scaler, rental_scaler):
    """INPUT: KNN parameters and required feature/target scaler instances.

    RETURN: A complete estimator containing temperature scaling, target
    scaling, and the KNN regressor. Predictions are returned in rental counts.
    """
    if temp_scaler is None or rental_scaler is None:
        raise ValueError("temp_scaler and rental_scaler must be provided")

    # DictVectorizer sorts these fields as Day, Hour, Month, temperature.
    # Keep temperature at index 3 so the scaler is applied to the intended field.
    feature_pipeline = Pipeline(
        [
            ("vectorizer", DictVectorizer(sparse=False, sort=True)),
            (
                "temperature_scaler",
                ColumnTransformer(
                    transformers=[("temperature", temp_scaler, [3])],
                    remainder="passthrough",
                ),
            ),
        ]
    )
    knn_pipeline = Pipeline(
        [
            ("features", feature_pipeline),
            ("knn", KNeighborsRegressor(**params)),
        ]
    )
    return TransformedTargetRegressor(
        regressor=knn_pipeline,
        transformer=rental_scaler,
    )


def objective(
    params,
    X_train_records,
    y_train_df,
    X_test_records,
    y_test_df,
    temp_scaler,
    rental_scaler,
):
    """INPUT: Hyperopt parameters, train/test frames, and fitted scalers.

    RETURN: A Hyperopt result dictionary containing test MSE and status.
    """
    params = params.copy()
    params["n_neighbors"] = int(params["n_neighbors"])
    params["leaf_size"] = int(params["leaf_size"])

    with mlflow.start_run(nested=True):
        # Build the complete KNN estimator from the current Hyperopt trial parameters.
        knn_model = build_knn_model(params, temp_scaler, rental_scaler)
        knn_model.fit(X_train_records, y_train_df.squeeze())

        # The packaged target transformer returns predictions in rental counts.
        predictions = knn_model.predict(X_test_records)
        mse = mean_squared_error(y_test_df.squeeze(), predictions)

        mlflow.log_params(params)
        mlflow.log_metric("test_mse", mse)
        mlflow.sklearn.log_model(
            sk_model=knn_model,
            artifact_path="knn_model",
            skops_trusted_types=True,
            serialization_format="skops"
        )  # Log this trial's packaged model securely.

    return {"loss": mse, "status": STATUS_OK}


@task
def run_hyperopt_search(
    X_train_records,
    y_train_df,
    X_test_records,
    y_test_df,
    temp_scaler,
    rental_scaler,
    data_path,
    dvc_data_hash,
    random_state,
    max_trials,
):
    """INPUT: Prepared train/test frames, scalers, data path, random seed, and trial limit.

    RETURN: The fitted best model, best parameters, and held-out test MSE.
    """
    search_space = {
        "n_neighbors": hp.quniform("n_neighbors", 1, 10, 1),
        "weights": hp.choice("weights", ["uniform", "distance"]),
        "algorithm": hp.choice("algorithm", ["ball_tree", "kd_tree"]),
        "leaf_size": hp.quniform("leaf_size", 10, 40, 5),
        "p": hp.choice("p", [1, 2]),
        "n_jobs": hp.choice("n_jobs", [None]),
    }

    trials = Trials()
    with mlflow.start_run(run_name="knn_regressor_hyperopt"):
        mlflow.log_param("data_path", data_path)  # Record the source dataset for reproducibility.
        mlflow.log_param("dvc_data_hash", dvc_data_hash)  # Record the exact DVC output version.
        mlflow.log_param("random_state", random_state)  # Record Hyperopt's search seed.
        mlflow.log_param("max_trials", max_trials)  # Record the Hyperopt trial limit.
        best_raw = fmin(
            fn=lambda params: objective(
                params,
                X_train_records,
                y_train_df,
                X_test_records,
                y_test_df,
                temp_scaler,
                rental_scaler,
            ),
            space=search_space,
            algo=tpe.suggest,
            max_evals=max_trials,
            trials=trials,
            rstate=np.random.default_rng(random_state),
            show_progressbar=False,
        )

    #     # Resolve Hyperopt's encoded choices into final estimator parameters.
    #     best_params = space_eval(search_space, best_raw)
    #     best_params["n_neighbors"] = int(best_params["n_neighbors"])
    #     best_params["leaf_size"] = int(best_params["leaf_size"])

    #     # Fit the selected parameters again for the parent-run summary metric.
    #     best_model = build_knn_model(best_params, temp_scaler, rental_scaler)
    #     best_model.fit(X_train_records, y_train_df.squeeze())
    #     predictions = best_model.predict(X_test_df)
    #     test_mse = mean_squared_error(y_test_df.squeeze(), predictions)

    #     mlflow.log_metric("best_test_mse", test_mse)

    # return best_model, best_params, test_mse


def parse_args():
    """RETURN: Command-line arguments for the training workflow."""
    parser = argparse.ArgumentParser(description="Run Hyperopt KNN training.")
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Seed used by Hyperopt.",
    )
    parser.add_argument(
        "--max-trials",
        type=int,
        default=50,
        help="Maximum number of Hyperopt trials.",
    )
    return parser.parse_args()


@flow
def main(random_state, max_trials):
    """INPUT: Hyperopt seed and maximum trial count.

    RETURN: None. Runs the KNN data-preparation and search workflow.
    """
    project_root = Path(__file__).resolve().parents[1]
    data_path = str(
        project_root
        / "ingestion/ingestion_run/output/bike_rental_hourly_data_2022_2025.csv"
    )
    dvc_lock_path = project_root / "dvc.lock"
    dvc_data_hash = get_dvc_output_hash(dvc_lock_path, data_path)
    df = load_data(data_path)
    (
        X_train_records,
        y_train_df,
        X_test_records,
        y_test_df,
        temp_scaler,
        rental_scaler,
    ) = prep_data_and_scaler(df)
    run_hyperopt_search(
        X_train_records,
        y_train_df,
        X_test_records,
        y_test_df,
        temp_scaler,
        rental_scaler,
        data_path,
        dvc_data_hash,
        random_state,
        max_trials,
    )


if __name__ == "__main__":
    args = parse_args()
    main(args.random_state, args.max_trials)
