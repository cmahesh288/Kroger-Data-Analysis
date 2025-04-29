# basket_analysis.py
import pandas as pd
import pyodbc
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from pathlib import Path
from config import CONN_STR

IMG_DIR = Path(__file__).parent / "static" / "images"
IMG_DIR.mkdir(parents=True, exist_ok=True)

def analysis_basket():
    with pyodbc.connect(CONN_STR, timeout=30) as conn:
        df = pd.read_sql_query("""
            SELECT TOP 500
              BASKET_NUM, PRODUCT_NUM
            FROM [400_transactions]
            WHERE BASKET_NUM IS NOT NULL
              AND PRODUCT_NUM IS NOT NULL
            ORDER BY NEWID()
        """, conn)

    top2 = (
        df.groupby("PRODUCT_NUM")["BASKET_NUM"]
          .nunique()
          .nlargest(2)
          .index
          .tolist()
    )
    if len(top2) < 2:
        return 0.0, None
    p1, p2 = top2

    pivot = (
        df[df["PRODUCT_NUM"].isin((p1, p2))]
        .assign(flag=1)
        .pivot_table(
            index="BASKET_NUM",
            columns="PRODUCT_NUM",
            values="flag",
            fill_value=0
        )
    )
    X = pivot[p1].values.reshape(-1, 1)
    y = pivot[p2].values

    from sklearn.linear_model import LogisticRegression
    model = LogisticRegression(solver="liblinear", max_iter=100)
    model.fit(X, y)
    acc = model.score(X, y)

    fig, ax = plt.subplots(figsize=(4,2))
    ax.barh([str(p1)], model.coef_[0])
    ax.set_title(f"Acc={acc:.2f}")
    plt.tight_layout()

    img_name = "basket_fast.png"
    fig.savefig(IMG_DIR / img_name, dpi=100)
    plt.close(fig)

    return acc, f"images/{img_name}"
