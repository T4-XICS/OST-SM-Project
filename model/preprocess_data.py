import pandas as pd
from sklearn.model_selection import train_test_split
from torch.utils.data import DataLoader

def preprocess_data(train, test, batch_size=32, sequence_length=30):
    if train.iloc[0].dtype == 'object':
        train = train[1:].copy()
        test = test[1:].copy()

    # convert all columns to numeric
    train = train.apply(pd.to_numeric, errors='coerce')
    test = test.apply(pd.to_numeric, errors='coerce')

    # interpolate/fill NaNs after conversion if necessary
    train.interpolate(inplace=True)
    train.bfill(inplace=True)
    test.interpolate(inplace=True)
    test.bfill(inplace=True)

    # calculate variance, crop columns with variance close to zero.
    variance_threshold = 1e-6
    variances = train.var()
    low_variance_cols = variances[variances < variance_threshold].index

    print(f"Columns with very low variance: {list(low_variance_cols)}")

    # drop low variance columns
    train_filtered = train.drop(columns=low_variance_cols)
    test_filtered = test.drop(columns=low_variance_cols)

    print(f"Original number of features: {train.shape[1]}")
    print(f"Number of features after removing low variance columns: {train_filtered.shape[1]}")

    # egenerate the tensors and dataloaders with the filtered data
    train_tensor = train_filtered.values
    test_tensor = test_filtered.values

    sequences = []
    for i in range(train_tensor.shape[0] - sequence_length + 1):
        sequences.append(train_tensor[i:i + sequence_length])

    train_data, val_data = train_test_split(sequences, test_size=0.3, random_state=42, shuffle=False) # 70% train, 30% temp

    test_sequences = []
    for i in range(test_tensor.shape[0] - sequence_length + 1):
        test_sequences.append(test_tensor[i:i + sequence_length])

    train_loader = DataLoader(dataset=train_data, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(dataset=val_data, batch_size=batch_size, shuffle=False)
    test_loader = DataLoader(dataset=test_sequences, batch_size=batch_size, shuffle=False)

    return train_loader, val_loader, test_loader
