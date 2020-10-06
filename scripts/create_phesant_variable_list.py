import fire
import pandas as pd


def run(input_path, output_path):
    df = pd.read_csv(input_path, sep="\t", dtype=str)
    # Mark variables as excluded unless their description contains any of these substrings
    terms = [
        "diabetes",
        "sleep",
        "depression",
        "cardiac",
        "addiction",
        "height",
        "weight",
    ]
    mask = pd.concat(
        [df["Field"].fillna("").str.lower().str.contains(term) for term in terms],
        axis=1,
    ).any(axis=1)
    df["EXCLUDED"] = df["EXCLUDED"].where(
        df["EXCLUDED"].notnull() | mask, "YES-NOT_IN_SUBSET"
    )
    df.to_csv(output_path, sep="\t", index=False, na_rep="")


if __name__ == "__main__":
    fire.Fire()
