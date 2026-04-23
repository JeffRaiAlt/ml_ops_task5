from pathlib import Path

import pandas as pd
import yaml
from sklearn.model_selection import train_test_split


def load_params(path: str = "params.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # удаляем полные дубли
    df = df.drop_duplicates()

    # пример базовой очистки строк
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    return df


def main() -> None:
    params = load_params()
    prepare_params = params["prepare"]

    input_path = prepare_params["input_path"]
    output_dir = Path(prepare_params["output_dir"])
    target = prepare_params["target"]
    test_size = prepare_params["test_size"]
    random_state = prepare_params["random_state"]

    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path)
    df = basic_cleaning(df)

    if target not in df.columns:
        raise ValueError(f"Target column '{target}' not found in dataset")

    train_df, test_df = train_test_split(
        df,
        test_size=test_size,
        random_state=random_state,
        stratify=df[target] if df[target].nunique() > 1 else None,
    )

    train_df.to_csv(output_dir / "train.csv", index=False)
    test_df.to_csv(output_dir / "test.csv", index=False)


if __name__ == "__main__":
    main()