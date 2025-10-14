import torch
import copy

def train_model(model, train_loader, val_loader, optimizer, loss_fn, scheduler, num_epochs=10, device='cpu'):
    torch.cuda.empty_cache()
    train_losses = []
    val_losses = []

    early_stop_tolerant_count = 0
    early_stop_tolerant = 10
    best_loss = float('inf')
    best_model_wts = copy.deepcopy(model.state_dict())

    for epoch in range(num_epochs):
        train_loss = 0.0
        model.train()
        for batch in train_loader:
            batch = torch.tensor(batch, dtype=torch.float32).to(device)

            optimizer.zero_grad()

            recon_batch, mean, logvar = model(batch)
            loss = loss_fn(recon_batch, batch, mean, logvar)

            loss.backward()
            optimizer.step()
            train_loss += loss.item()

        train_loss /= len(train_loader)
        train_losses.append(train_loss)

        # Validation
        model.eval()
        valid_loss = 0.0
        with torch.no_grad():
            for batch in val_loader:
                batch = torch.tensor(batch, dtype=torch.float32).to(device)
                recon_batch, mean, logvar = model(batch)
                loss = loss_fn(recon_batch, batch, mean, logvar)
                valid_loss += loss.item()

        valid_loss /= len(val_loader)
        val_losses.append(valid_loss)

        scheduler.step(valid_loss)

        if valid_loss < best_loss:
            best_loss = valid_loss
            best_model_wts = copy.deepcopy(model.state_dict())
            early_stop_tolerant_count = 0
        else:
            early_stop_tolerant_count += 1

        print(f"Epoch {epoch+1:04d}: train loss {train_loss:.4f}, valid loss {valid_loss:.4f}")

        if early_stop_tolerant_count >= early_stop_tolerant:
            print("Early stopping triggered.")
            break

    model.load_state_dict(best_model_wts)
    print("Finished Training.")
    return train_losses, val_losses
