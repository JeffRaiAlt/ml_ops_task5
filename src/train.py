from pathlib import Path

import joblib
import pandas as pd
import yaml
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_experiment("bank-churn-local")


def load_params(path: str = "params.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_model(params: dict):
    train_params = params["train"]
    model_type = train_params["model_type"]
    random_state = train_params["random_state"]

    if model_type == "logistic_regression":
        lr_params = train_params["logistic_regression"]
        model = LogisticRegression(
            C=lr_params["C"],
            max_iter=lr_params["max_iter"],
            random_state=random_state,
        )
    elif model_type == "random_forest":
        rf_params = train_params["random_forest"]
        model = RandomForestClassifier(
            n_estimators=rf_params["n_estimators"],
            max_depth=rf_params["max_depth"],
            min_samples_split=rf_params["min_samples_split"],
            random_state=random_state,
        )
    else:
        raise ValueError(f"Unsupported model_type: {model_type}")

    return model


def main() -> None:
    params = load_params()

    prepare_params = params["prepare"]
    train_params = params["train"]

    data_dir = Path(prepare_params["output_dir"])
    target = train_params["target"]
    model_path = Path(train_params["model_path"])

    train_df = pd.read_csv(data_dir / "train.csv")

    if target not in train_df.columns:
        raise ValueError(f"Target column '{target}' not found in train data")

    X_train = train_df.drop(columns=[target])
    y_train = train_df[target]

    numeric_features = X_train.select_dtypes(include=["number"]).columns.tolist()
    categorical_features = X_train.select_dtypes(exclude=["number"]).columns.tolist()

    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_features),
            ("cat", categorical_transformer, categorical_features),
        ]
    )

    model = build_model(params)

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    # Обертка
    with mlflow.start_run(run_name="train"):
        # параметры
        mlflow.log_param("model_type", train_params["model_type"])
        mlflow.log_param("random_state", train_params["random_state"])

        if train_params["model_type"] == "random_forest":
            mlflow.log_params(train_params["random_forest"])
        elif train_params["model_type"] == "logistic_regression":
            mlflow.log_params(train_params["logistic_regression"])

        train_dataset = mlflow.data.from_pandas(
            df=train_df,
            source=str(data_dir / "train.csv"),
            name="train_dataset",
            targets=target,
        )
        mlflow.log_input(train_dataset, context="training")

        pipeline.fit(X_train, y_train)

        joblib.dump(pipeline, model_path)
        mlflow.log_artifact(str(model_path))

        y_pred_train = pipeline.predict(X_train.head(50))
        signature = infer_signature(X_train.head(50), y_pred_train)

        mlflow.sklearn.log_model(
            sk_model=pipeline,
            name="model",
            signature=signature,
            registered_model_name="bank-churn-model",
        )


if __name__ == "__main__":
    main()