from pathlib import Path
import pytest
import csv
import pandas as pd


@pytest.fixture(scope="module")
def data_vaccines():
    r"""
    Open existing vacines data
    """

    current_dir = Path(__file__).parent.absolute()
    csv_filepath = current_dir / ".." / "vacinacao.csv"
    data = pd.read_csv(csv_filepath)

    return data



def test_number_of_columns(data_vaccines):
    """
    Generated CSV should always have same number of columns: 9
    """

    for i, row in data_vaccines.iterrows():
        assert len(row) == 9

def test_sequencial_dates(data_vaccines):
    """
    Check if the dates are sequencial
    """

    for i, row in data_vaccines.iterrows():
        if i>= 1:
            today_date = data_vaccines.iloc[i]["Data"]
            yesterday_date = data_vaccines.iloc[i-1]["Data"]
            diff_date = (today_date - yesterday_date).days
            assert diff_date == 1
