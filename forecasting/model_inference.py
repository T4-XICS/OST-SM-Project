import os, time, threading
import torch

import logging

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from model.network import LSTMVAE 

model_path = "/weights/lstm_vae_swat.pth"
current_model = None
last_modified_time = None
model_lock = threading.Lock()

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logging.info(f"Using device: {device}")

FORECAST_HORIZON = 10
INPUT_DIM = 2
SEQ_LEN = 30

model_lock = threading.Lock()

def load_model_safe():
    """
    Loads the LSTMVAE model safely with metadata from checkpoint.
    """
    global current_model, last_modified_time

    try:
        if not os.path.exists(model_path):
            logging.warning(f"Model path {model_path} does not exist.")
            return None

        logging.info(f"Loading model from {model_path}")
        checkpoint = torch.load(model_path, map_location=device)

        # Extract metadata and instantiate model
        model = LSTMVAE(
            input_dim=checkpoint['input_dim'],
            hidden_dim=checkpoint['hidden_dim'],
            latent_dim=checkpoint['latent_dim'],
            sequence_length=checkpoint['sequence_length'],
            num_layers=1,
            device=device
        )

        # Load state dict
        model.load_state_dict(checkpoint['state_dict'])
        model.to(device)
        model.eval()

        # Update globals safely
        with model_lock:
            current_model = model
            last_modified_time = os.path.getmtime(model_path)

        logging.info("Model loaded successfully.")
        return current_model

    except Exception as e:
        logging.error(f"Failed to load model: {e}")
        return None

def _reload_if_needed():
    """Check if model file changed and reload."""
    global last_modified_time
    if not os.path.exists(model_path):
        return
    try:
        mtime = os.path.getmtime(model_path)
        if last_modified_time is None or mtime > last_modified_time:
            logging.info("Model weights updated — reloading.")
            load_model_safe()
    except Exception as e:
        logging.error(f"Error checking model: {e}")


def start_model_reload_thread(interval=60):
    """
    Background thread to periodically reload model weights.
    """
    def _worker():
        logging.info(f"Model reload thread started (interval = {interval}s)")
        while True:
            try:
                _reload_if_needed()
            except Exception as e:
                logging.error(f"Model reload error: {e}")
            finally:
                import time
                time.sleep(interval)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()



def predict_forecast(input_tensor, device='cpu'):
    model = load_model_safe().to(device)
    model.eval()
    with torch.no_grad():
        output = model(input_tensor.to(device))
        
        # If model returns tuple (output, extra), take only the first element
        if isinstance(output, tuple):
            output = output[0]

        return output.cpu().numpy()
