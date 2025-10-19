import pandas as pd
import numpy as np

def read_dataset():
    DATASET = "datasets/swat/"
    TRAIN_FILE_NAME = "SWaT_Dataset_Normal_v0 1.csv"
    TRAIN_DATASET = DATASET + TRAIN_FILE_NAME
    TEST_FILE_NAME = "SWaT_Dataset_Attack_v0 1.csv"
    TEST_DATASET = DATASET + TEST_FILE_NAME

    data_train = pd.read_csv(TRAIN_DATASET, header=None)
    data_test = pd.read_csv(TEST_DATASET, header=None)

    data_train.replace("Normal", 0, inplace=True)
    data_test.replace("Normal", 0, inplace=True)
    data_test.replace("Attack", 1, inplace=True)
    # Typo within the dataset
    data_test.replace("A ttack", 1, inplace=True)

    train = data_train.iloc[:, 1:].copy()
    train = train.iloc[:, :-1].copy()
    test = data_test.iloc[:, 1:].copy()
    test = test.iloc[:, :-1].copy()

    true_anomalies = data_test[52].to_numpy()
    true_anomalies = np.delete(true_anomalies, 0)
    true_anomalies = true_anomalies.astype(int)

    return train, test, true_anomalies
