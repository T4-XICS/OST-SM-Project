import torch
import torch.nn as nn

class LSTMForecaster(nn.Module):
    def __init__(self, input_dim, hidden_dim=64, num_layers=2, forecast_horizon=10):
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_dim, input_dim * forecast_horizon)

        self.forecast_horizon = forecast_horizon
        self.input_dim = input_dim

    def forward(self, x):
        out, _ = self.lstm(x)
        last_out = out[:, -1, :]
        pred = self.fc(last_out)
        pred = pred.view(-1, self.forecast_horizon, self.input_dim)
        return pred
