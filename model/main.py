#!/usr/bin/env python

import torch
from torch.optim import Adam
from torch.optim.lr_scheduler import ReduceLROnPlateau
from sklearn.metrics import roc_auc_score, classification_report, confusion_matrix

from read_csv import read_dataset
from preprocess_data import preprocess_data
from network import LSTMVAE, loss_function, save_model
from train import train_model
from evaluate import evaluate_lstm, calculate_f1_score

def main():
    # Setup the dataset
    train, test, true_anomalies = read_dataset()

    # Preprocess the Dataset
    sequence_length = 30
    train_loader, val_loader, test_loader = preprocess_data(train, test, batch_size=32, sequence_length=sequence_length)

    input_dim = train_loader.dataset[0].shape[1]
    hidden_dim = 128
    latent_dim = 32
    num_layers = 1

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = LSTMVAE(input_dim=input_dim,
                    hidden_dim=hidden_dim,
                    latent_dim=latent_dim,
                    sequence_length=sequence_length,
                    num_layers=num_layers,
                    device=device).to(device)
    optimizer = Adam(model.parameters(), lr=1e-3)
    scheduler = ReduceLROnPlateau(optimizer, 'min', patience=5, factor=0.1)

    # Train
    torch.cuda.empty_cache()

    train_model(model, train_loader, val_loader, optimizer, loss_function, scheduler, num_epochs=1, device=device)

    save_model(model, "lstm_vae_swat", input_dim, latent_dim, hidden_dim, sequence_length)


    # Evaluate
    anomalies = evaluate_lstm(model, test_loader, device, 90)

    f1, predicted_anomalies = calculate_f1_score(anomalies, true_anomalies)
    print(f"F1 Score: {f1}")

    auc_roc = roc_auc_score(true_anomalies, predicted_anomalies)
    print(f"AUC-ROC Score: {auc_roc}")

    print(classification_report(true_anomalies, predicted_anomalies))

    print(confusion_matrix(true_anomalies, predicted_anomalies))

if __name__ == "__main__":
    main()
