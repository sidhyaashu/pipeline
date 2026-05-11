import pandas as pd
from app.config import ALLOW_MASS_DELETE

def validate_payload_df(df: pd.DataFrame, table_name: str, pk_cols: list[str]) -> dict:
    result = {
        "valid": True,
        "errors": [],
        "warnings": []
    }

    if df.empty:
        return result
        
    df_columns_upper = [c.upper() for c in df.columns]

    if "FLAG" not in df_columns_upper:
        result["valid"] = False
        result["errors"].append("Missing FLAG column")
    else:
        flag_col = df.columns[df_columns_upper.index("FLAG")]
        invalid_flags = df[~df[flag_col].isin(['A', 'O', 'D'])]
        if not invalid_flags.empty:
            result["valid"] = False
            result["errors"].append("Invalid FLAG values found. Only A/O/D allowed.")
            
        d_count = len(df[df[flag_col] == 'D'])
        if d_count > 0:
            delete_ratio = d_count / len(df)
            if len(df) > 50 and delete_ratio > 0.4:
                if not ALLOW_MASS_DELETE:
                    result["valid"] = False
                    result["errors"].append(
                        f"Mass delete blocked: {d_count}/{len(df)} rows ({delete_ratio:.0%})"
                    )
                else:
                    result["warnings"].append(
                        f"Mass delete detected: {d_count}/{len(df)} rows ({delete_ratio:.0%})"
                    )
            elif delete_ratio > 0.4:
                result["warnings"].append(
                    f"High delete ratio in small batch: {d_count}/{len(df)} rows ({delete_ratio:.0%})"
                )
            
    for pk in pk_cols:
        if pk.upper() not in df_columns_upper:
            result["valid"] = False
            result["errors"].append(f"Missing primary key column: {pk}")
        else:
            pk_actual_col = df.columns[df_columns_upper.index(pk.upper())]
            null_pks = df[df[pk_actual_col].isnull()]
            if not null_pks.empty:
                result["valid"] = False
                result["errors"].append(f"Null values found in primary key column: {pk}")
                
    return result
