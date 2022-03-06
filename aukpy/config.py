from pathlib import Path
from os import getenv


DATA_HOME = Path(getenv("XDG_DATA_HOME", Path.home() / ".local" / "share" / "aukpy"))
