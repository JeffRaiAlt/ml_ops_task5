from datetime import timedelta
import pandas as pd
from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    Project,
    RequestSource,
    ValueType,
)

from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, Json, Map, String, Struct

# 1. Проект (должен совпадать с feature_store.yaml)
project = Project(name="my_feature_store_prg", description="Проект на PostgreSQL")

# 2. Сущность
driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)

# 3. НОВЫЙ ИСТОЧНИК (PostgreSQL вместо FileSource)
driver_stats_source = PostgreSQLSource(
    name="driver_hourly_stats_source",
    table="feast_driver_hourly_stats", # Имя таблицы из вашего seed_postgres.py
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# 4. Feature View
driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
        # Заменяем сложные типы на String
        Field(name="driver_metadata", dtype=String),
        Field(name="driver_config", dtype=String),
        Field(name="driver_profile", dtype=String),
    ],
    online=True,
    source=driver_stats_source, # Ссылка на SQL источник
    tags={"team": "driver_performance"},
)

# 5. Request Source (данные из HTTP запроса)
input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name="val_to_add", dtype=Int64),
        Field(name="val_to_add_2", dtype=Int64),
    ],
)

# 6. On-demand Feature View (вычисления на лету)
@on_demand_feature_view(
    sources=[driver_stats_fv, input_request],
    schema=[
        Field(name="conv_rate_plus_val1", dtype=Float64),
        Field(name="conv_rate_plus_val2", dtype=Float64),
    ],
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]
    return df

# 7. Сервис (точка доступа для модели)
driver_activity_v1 = FeatureService(
    name="driver_activity_v1",
    features=[driver_stats_fv, transformed_conv_rate],
)
