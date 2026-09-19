from sklearn.neighbors import KNeighborsRegressor
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction import DictVectorizer
import mlflow
import mlflow.sklearn

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
mlflow.sklearn.autolog(disable=True)  # Keep MLflow logging manual for each trial.


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
def prep_data(df):
    """INPUT: Raw bike-rental DataFrame.

    RETURN: Chronologically split train/test feature and target DataFrames.
    """
    prepared_df = df.copy()

    prepared_df["datetime"] = pd.to_datetime(
        prepared_df[["Year", "Month", "Day", "Hour"]]
    )
    prepared_df = prepared_df.sort_values("datetime").reset_index(drop=True)
    prepared_df["total_classic_bike_rental"] = (
        prepared_df["classic_bike_casual_count"]
        + prepared_df["classic_bike_member_count"]
    )

    feature_columns = ["Month", "Day", "Hour", "temperature"]
    train_mask = prepared_df["Year"] != 2025
    train_df = prepared_df.loc[train_mask].copy()
    test_df = prepared_df.loc[~train_mask].copy()

    return (
        train_df[feature_columns].copy(),
        train_df[["total_classic_bike_rental"]].copy(),
        test_df[feature_columns].copy(),
        test_df[["total_classic_bike_rental"]].copy(),
    )


def prep_scaler(X_train, y_train, columns_to_scale, scale_target):
    """INPUT: Training features/target, feature columns, and target-scale flag.

    RETURN: List of (column name, scaler, DictVectorizer column index) tuples,
    and a fitted target scaler or None.
    """
    missing_columns = [column for column in columns_to_scale if column not in X_train.columns]
    if missing_columns:
        raise ValueError(f"Columns not found in X_train: {missing_columns}")

    sorted_columns = sorted(X_train.columns)
    feature_scalers = []
    for column in columns_to_scale:
        scaler = StandardScaler()
        scaler.fit(X_train[[column]])
        feature_scalers.append((column, scaler, sorted_columns.index(column)))

    target_scaler = None
    if scale_target:
        target_scaler = StandardScaler()
        target_scaler.fit(y_train)

    return feature_scalers, target_scaler


def build_knn_model(params, feature_scalers, target_scaler):
    """INPUT: KNN parameters, feature scaler tuples, and optional target scaler.

    RETURN: A KNN estimator containing the requested feature scalers and, when
    provided, the target scaler for automatic inverse transformation.
    """
    feature_steps = [("vectorizer", DictVectorizer(sparse=False, sort=True))]
    if feature_scalers:
        feature_steps.append(
            (
                "column_scalers",
                ColumnTransformer(
                    transformers=[
                        (column, scaler, [column_index])
                        for column, scaler, column_index in feature_scalers
                    ],
                    remainder="passthrough",
                ),
            )
        )

    knn_pipeline = Pipeline(
        feature_steps + [("knn", KNeighborsRegressor(**params))]
    )
    if target_scaler is None:
        return knn_pipeline
    return TransformedTargetRegressor(
        regressor=knn_pipeline,
        transformer=target_scaler,
    )


def objective(
    params,
    X_train_df,
    y_train_df,
    X_test_df,
    y_test_df,
    feature_scalers,
    target_scaler,
    run_metadata,
):
    """INPUT: Hyperopt parameters, train/test frames, scalers, and run metadata.

    RETURN: A Hyperopt result dictionary containing test MSE and status.
    """
    params = params.copy()
    params["n_neighbors"] = int(params["n_neighbors"])
    params["leaf_size"] = int(params["leaf_size"])

    with mlflow.start_run(nested=True):
        # Convert DataFrames to records only at the DictVectorizer model boundary.
        X_train_records = X_train_df.to_dict(orient="records")
        X_test_records = X_test_df.to_dict(orient="records")
        knn_model = build_knn_model(params, feature_scalers, target_scaler)
        knn_model.fit(X_train_records, y_train_df.squeeze())

        # The packaged target transformer returns predictions in rental counts.
        predictions = knn_model.predict(X_test_records)
        mse = mean_squared_error(y_test_df.squeeze(), predictions)

        mlflow.log_params(params)
        mlflow.log_params(run_metadata)  # Record shared metadata for this trial.
        mlflow.log_metric("test_mse", mse)
        mlflow.sklearn.log_model(
            sk_model=knn_model,
            artifact_path="knn_model",
            serialization_format="skops",
            skops_trusted_types=[
                "sklearn.metrics._dist_metrics.EuclideanDistance64",
                "sklearn.metrics._dist_metrics.ManhattanDistance64",
                "sklearn.neighbors._kd_tree.KDTree",
                "sklearn.neighbors._ball_tree.BallTree",
            ],
        )  # Log this trial's packaged model securely.

    return {"loss": mse, "status": STATUS_OK}


@task
def run_hyperopt_search(
    X_train_df,
    y_train_df,
    X_test_df,
    y_test_df,
    feature_scalers,
    target_scaler,
    run_metadata,
    max_trials,
):
    """INPUT: Prepared train/test frames, scalers, run metadata, and trial limit.

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

    random_state = run_metadata["random_state"]
    trials = Trials()
    best_raw = fmin(
        fn=lambda params: objective(
            params,
            X_train_df,
            y_train_df,
            X_test_df,
            y_test_df,
            feature_scalers,
            target_scaler,
            run_metadata,
        ),
        space=search_space,
        algo=tpe.suggest,
        max_evals=max_trials,
        trials=trials,
        rstate=np.random.default_rng(random_state),
        show_progressbar=False,
    )



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

    columns_to_scale=["Month","Day","Hour","temperature"]
    scale_target = True

    run_metadata = {
            "data_path": data_path,
            "dvc_data_hash": dvc_data_hash,
            "random_state": random_state,
            "columns_to_scale":columns_to_scale,
            "scale_target": scale_target
    }
    
    (
        X_train_df,
        y_train_df,
        X_test_df,
        y_test_df,
        feature_scalers,
        target_scaler,
    ) = prep_data(df)
    feature_scalers, target_scaler = prep_scaler(
        X_train_df,
        y_train_df,
        columns_to_scale=columns_to_scale,
        scale_target=scale_target,
    )
    
    run_hyperopt_search(
        X_train_df,
        y_train_df,
        X_test_df,
        y_test_df,
        feature_scalers,
        target_scaler,
        run_metadata,
        max_trials,
    )


if __name__ == "__main__":
    args = parse_args()
    main(args.random_state, args.max_trials)
