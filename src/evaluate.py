import json
from pathlib import Path

import joblib
import mlflow
import pandas as pd
import yaml
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_experiment("bank-churn-local")


def load_params(path: str = "params.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_test_data(data_dir: Path, target: str) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    test_df = pd.read_csv(data_dir / "test.csv")

    if target not in test_df.columns:
        raise ValueError(f"Target column '{target}' not found in test data")

    X_test = test_df.drop(columns=[target])
    y_test = test_df[target]
    return test_df, X_test, y_test


def load_model(model_path: Path):
    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")
    return joblib.load(model_path)

@mlflow.trace(name="evaluate_metrics", attributes={"stage": "evaluation"})
def collect_metrics(y_true: pd.Series, y_pred, y_proba=None) -> dict:
    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
    }

    if y_proba is not None:
        metrics["roc_auc"] = float(roc_auc_score(y_true, y_proba))

    return metrics


def predict(model, X_test: pd.DataFrame, y_test: pd.Series) -> dict:
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else None
    metrics = collect_metrics(y_test, y_pred, y_proba)

    return metrics


def save_metrics(metrics: dict, metrics_path: Path) -> None:
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)


def log_test_dataset(test_df: pd.DataFrame, data_dir: Path, target: str) -> None:
    test_dataset = mlflow.data.from_pandas(
        df=test_df,
        source=str(data_dir / "test.csv"),
        name="test_dataset",
        targets=target,
    )
    mlflow.log_input(test_dataset, context="evaluation")


def main() -> None:
    params = load_params()
    prepare_params = params["prepare"]
    train_params = params["train"]

    data_dir = Path(prepare_params["output_dir"])
    target = train_params["target"]
    model_path = Path(train_params["model_path"])
    metrics_path = Path("report/metrics.json")

    test_df, X_test, y_test = load_test_data(data_dir, target)
    model = load_model(model_path)

    with mlflow.start_run(run_name="evaluate"):
        log_test_dataset(test_df, data_dir, target)

        metrics = predict(model, X_test, y_test)

        save_metrics(metrics, metrics_path)
        print(metrics)
        mlflow.log_metrics(metrics)
        mlflow.log_artifact(str(metrics_path))


if __name__ == "__main__":
    main()