import torch
import numpy as np
from network import loss_function
from sklearn.metrics import f1_score

def evaluate_lstm(model, test_loader, device, percentile_threshold=90):
    model.eval()
    anomaly_scores = []

    with torch.no_grad():
        for batch in test_loader:
            batch = torch.tensor(batch, dtype=torch.float32).to(device)

            batch_scores = []
            for i in range(batch.shape[0]): #Iterate through each sequence in the batch
                sequence = batch[i, :, :].unsqueeze(0)  # Select a single sequence
                recon_batch, mean, logvar = model(sequence)
                loss = loss_function(recon_batch, sequence, mean, logvar)
                batch_scores.append(loss.item())
            anomaly_scores.extend(batch_scores)  # Append scores for all sequences in the batch

    # Calculate the threshold based on the specified percentile
    threshold = np.percentile(anomaly_scores, percentile_threshold)

    # Identify anomaly indices
    anomaly_indices = [i for i, score in enumerate(anomaly_scores) if score > threshold]
    return anomaly_indices


def calculate_f1_score(anomaly_indices, true_anomalies):
    # Create a binary array representing predicted anomalies
    predicted_anomalies = np.zeros_like(true_anomalies)
    for index in anomaly_indices:
        if index < len(predicted_anomalies):  # Check index bounds
          predicted_anomalies[index] = 1

    # Calculate the F1 score
    f1 = f1_score(true_anomalies, predicted_anomalies)
    return f1, predicted_anomalies
