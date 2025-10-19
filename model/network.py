import torch
import torch.nn as nn

class LSTMEncoder(nn.Module):
    def __init__(self, input_dim, hidden_dim, latent_dim, num_layers=1):
        super(LSTMEncoder, self).__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers=num_layers, batch_first=True)
        self.fc_mean = nn.Linear(hidden_dim, latent_dim)
        self.fc_logvar = nn.Linear(hidden_dim, latent_dim)

    def forward(self, x):
        _, (h_n, _) = self.lstm(x)  # h_n: (num_layers, batch, hidden_dim)
        h = h_n[-1]  # take the output of the last layer
        return self.fc_mean(h), self.fc_logvar(h)


class LSTMDecoder(nn.Module):
    def __init__(self, latent_dim, hidden_dim, output_dim, sequence_length, num_layers=1):
        super(LSTMDecoder, self).__init__()
        self.sequence_length = sequence_length
        self.latent_to_hidden = nn.Linear(latent_dim, hidden_dim)
        self.lstm = nn.LSTM(hidden_dim, hidden_dim, num_layers=num_layers, batch_first=True)
        self.output_layer = nn.Linear(hidden_dim, output_dim)

    def forward(self, z):
        # Repeat z for each timestep
        hidden = self.latent_to_hidden(z).unsqueeze(1).repeat(1, self.sequence_length, 1)
        out, _ = self.lstm(hidden)
        return self.output_layer(out)


class LSTMVAE(nn.Module):
    def __init__(self, input_dim, hidden_dim, latent_dim, sequence_length, num_layers=1, device='cpu'):
        super(LSTMVAE, self).__init__()
        self.encoder = LSTMEncoder(input_dim, hidden_dim, latent_dim, num_layers).to(device)
        self.decoder = LSTMDecoder(latent_dim, hidden_dim, input_dim, sequence_length, num_layers).to(device)

    def reparameterize(self, mean, logvar):
        std = torch.exp(0.5 * logvar)
        eps = torch.randn_like(std)
        return mean + eps * std

    def forward(self, x):
        mean, logvar = self.encoder(x)
        z = self.reparameterize(mean, logvar)
        x_recon = self.decoder(z)
        return x_recon, mean, logvar
    
def loss_function(x, x_hat, mean, log_var):
    reproduction_loss = nn.functional.mse_loss(x_hat, x, reduction='sum')
    KLD = - 0.5 * torch.sum(1+ log_var - mean.pow(2) - log_var.exp())

    return reproduction_loss + KLD

def save_model(model, name, input_dim, latent_dim, hidden_dim, sequence_length):
    model_state = {
        'input_dim':input_dim,
        'latent_dim':latent_dim,
        'hidden_dim':hidden_dim,
        'sequence_length':sequence_length,
        'state_dict':model.state_dict()
    }
    torch.save(model_state, name + '.pth')
