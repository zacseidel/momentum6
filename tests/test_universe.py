import pandas as pd
from pathlib import Path
from universe import UniverseService

SAMPLE_SPY = """Ticker,Security Name,Weight
AAPL,Apple Inc.,7.50
MSFT,Microsoft Corp.,6.70
GOOG,Alphabet Inc. C,2.10
GOOGL,Alphabet Inc. A,2.05
"""
SAMPLE_MDY = """Ticker,Security Name,Weight
POOL,Pool Corp.,0.80
FND,Floor & Decor,0.75
"""

def fake_download(self, client, ticker):
    import io, pandas as pd
    csv = SAMPLE_SPY if ticker == "SPY" else SAMPLE_MDY
    return pd.read_csv(io.StringIO(csv))

async def test_sync(tmp_path, monkeypatch):
    svc = UniverseService(data_dir=tmp_path)
    monkeypatch.setattr(UniverseService, "_download_holdings", fake_download)
    await svc.sync()
    # files exist
    assert (tmp_path / "sp500.csv").exists()
    assert (tmp_path / "sp400.csv").exists()
    assert (tmp_path / "megacap.csv").exists()
    # megacap size == 25 capped or len of sample
    assert len(pd.read_csv(tmp_path / "megacap.csv")) <= 25
    # change log has rows
    assert not svc.get_change_log().empty